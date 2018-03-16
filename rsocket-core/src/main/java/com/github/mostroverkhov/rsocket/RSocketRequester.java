/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.mostroverkhov.rsocket;

import com.github.mostroverkhov.rsocket.exceptions.Exceptions;
import com.github.mostroverkhov.rsocket.internal.LimitableRequestPublisher;
import com.github.mostroverkhov.rsocket.internal.UnboundedProcessor;
import com.github.mostroverkhov.rsocket.util.ExceptionUtil;
import com.github.mostroverkhov.rsocket.util.NonBlockingHashMapLong;
import com.github.mostroverkhov.rsocket.util.PayloadImpl;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.*;

/** Client Side of a RSocket socket. Sends {@link Frame}s to a {@link RSocketResponder} */
class RSocketRequester implements RSocket {
  private static final ClosedChannelException CLOSED_CHANNEL_EXCEPTION =
      ExceptionUtil.noStacktrace(new ClosedChannelException());

  private final DuplexConnection connection;
  private final Consumer<Throwable> errorConsumer;
  private final StreamIdSupplier streamIdSupplier;
  private final MonoProcessor<Void> started;
  private final NonBlockingHashMapLong<LimitableRequestPublisher> senders;
  private final NonBlockingHashMapLong<UnicastProcessor<Payload>> receivers;
  private final UnboundedProcessor<Frame> sendProcessor;
  private volatile Throwable closedError;

  RSocketRequester(
      DuplexConnection connection,
      Consumer<Throwable> errorConsumer,
      StreamIdSupplier streamIdSupplier) {
    this.connection = connection;
    this.errorConsumer = errorConsumer;
    this.streamIdSupplier = streamIdSupplier;
    this.started = MonoProcessor.create();
    this.senders = new NonBlockingHashMapLong<>(256);
    this.receivers = new NonBlockingHashMapLong<>(256);

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    this.sendProcessor = new UnboundedProcessor<>();

    connection.onClose().doFinally(signalType -> cleanup()).subscribe(null, errorConsumer);

    connection
        .send(sendProcessor)
        .doFinally(this::handleSendProcessorCancel)
        .subscribe(null, this::handleSendProcessorError);

    connection
        .receive()
        .doOnSubscribe(subscription -> started.onComplete())
        .subscribe(this::handleIncomingFrames, errorConsumer);
  }

  private void handleSendProcessorError(Throwable t) {
    for (Subscriber subscriber : receivers.values()) {
      try {
        subscriber.onError(t);
      } catch (Throwable e) {
        errorConsumer.accept(e);
      }
    }

    for (LimitableRequestPublisher p : senders.values()) {
      p.cancel();
    }
  }

  private void handleSendProcessorCancel(SignalType t) {
    if (SignalType.ON_ERROR == t) {
      return;
    }

    for (Subscriber subscriber : receivers.values()) {
      try {
        subscriber.onError(new Throwable("closed connection"));
      } catch (Throwable e) {
        errorConsumer.accept(e);
      }
    }

    for (LimitableRequestPublisher p : senders.values()) {
      p.cancel();
    }
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return request(handleFireAndForget(payload), Mono::error);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return request(handleRequestResponse(payload), Mono::error);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return request(handleRequestStream(payload), Flux::error);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return request(handleChannel(Flux.from(payloads), FrameType.REQUEST_CHANNEL), Flux::error);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return request(handleMetadataPush(payload), Mono::error);
  }

  @Override
  public double availability() {
    return connection.availability();
  }

  @Override
  public Mono<Void> close() {
    return connection.close();
  }

  @Override
  public Mono<Void> onClose() {
    return connection.onClose();
  }

  private <T, K extends Publisher<T>> K request(K request, Function<Throwable, K> errF) {
    Throwable err = closedError;
    return err != null ? errF.apply(err) : request;
  }

  private Mono<Void> handleMetadataPush(Payload payload) {
    return started.then(
        Mono.fromRunnable(
            () -> {
              final Frame requestFrame = Frame.Request.from(0, FrameType.METADATA_PUSH, payload, 1);
              sendProcessor.onNext(requestFrame);
            }));
  }

  private Mono<Void> handleFireAndForget(Payload payload) {

    return started.then(
        Mono.fromRunnable(
            () -> {
              final int streamId = streamIdSupplier.nextStreamId();
              final Frame requestFrame =
                  Frame.Request.from(streamId, FrameType.FIRE_AND_FORGET, payload, 1);
              sendProcessor.onNext(requestFrame);
            }));
  }

  private Flux<Payload> handleRequestStream(final Payload payload) {
    return started.thenMany(
        Flux.defer(
            () -> {
              int streamId = streamIdSupplier.nextStreamId();

              UnicastProcessor<Payload> receiver = UnicastProcessor.create();
              receivers.put(streamId, receiver);

              AtomicBoolean first = new AtomicBoolean(false);

              return receiver
                  .doOnRequest(
                      l -> {
                        if (first.compareAndSet(false, true) && !receiver.isDisposed()) {
                          final Frame requestFrame =
                              Frame.Request.from(streamId, FrameType.REQUEST_STREAM, payload, l);
                          sendProcessor.onNext(requestFrame);
                        } else if (contains(streamId) && !receiver.isDisposed()) {
                          sendProcessor.onNext(Frame.RequestN.from(streamId, l));
                        }
                        sendProcessor.drain();
                      })
                  .doOnError(
                      t -> {
                        if (contains(streamId) && !receiver.isDisposed()) {
                          sendProcessor.onNext(Frame.Error.from(streamId, t));
                        }
                      })
                  .doOnCancel(
                      () -> {
                        if (contains(streamId) && !receiver.isDisposed()) {
                          sendProcessor.onNext(Frame.Cancel.from(streamId));
                        }
                      })
                  .doFinally(
                      s -> {
                        receivers.remove(streamId);
                      });
            }));
  }

  private Mono<Payload> handleRequestResponse(final Payload payload) {
    return started.then(
        Mono.defer(
            () -> {
              int streamId = streamIdSupplier.nextStreamId();
              final Frame requestFrame =
                  Frame.Request.from(streamId, FrameType.REQUEST_RESPONSE, payload, 1);

              UnicastProcessor<Payload> receiver = UnicastProcessor.create();
              receivers.put(streamId, receiver);

              sendProcessor.onNext(requestFrame);

              return receiver
                  .singleOrEmpty()
                  .doOnError(t -> sendProcessor.onNext(Frame.Error.from(streamId, t)))
                  .doOnCancel(() -> sendProcessor.onNext(Frame.Cancel.from(streamId)))
                  .doFinally(
                      s -> {
                        receivers.remove(streamId);
                      });
            }));
  }

  private Flux<Payload> handleChannel(Flux<Payload> request, FrameType requestType) {
    return started.thenMany(
        Flux.defer(
            new Supplier<Flux<Payload>>() {
              final UnicastProcessor<Payload> receiver = UnicastProcessor.create();
              final int streamId = streamIdSupplier.nextStreamId();
              volatile @Nullable MonoProcessor<Void> subscribedRequests;
              boolean firstRequest = true;

              boolean isValidToSendFrame() {
                return contains(streamId) && !receiver.isDisposed();
              }

              void sendOneFrame(Frame frame) {
                if (isValidToSendFrame()) {
                  sendProcessor.onNext(frame);
                }
              }

              @Override
              public Flux<Payload> get() {
                return receiver
                    .doOnRequest(
                        l -> {
                          boolean _firstRequest = false;
                          synchronized (RSocketRequester.this) {
                            if (firstRequest) {
                              _firstRequest = true;
                              firstRequest = false;
                            }
                          }

                          if (_firstRequest) {
                            AtomicBoolean firstPayload = new AtomicBoolean(true);
                            Flux<Frame> requestFrames =
                                request
                                    .transform(
                                        f -> {
                                          LimitableRequestPublisher<Payload> wrapped =
                                              LimitableRequestPublisher.wrap(f);
                                          // Need to set this to one for first the frame
                                          wrapped.increaseRequestLimit(1);
                                          senders.put(streamId, wrapped);
                                          receivers.put(streamId, receiver);

                                          return wrapped;
                                        })
                                    .map(
                                        new Function<Payload, Frame>() {

                                          @Override
                                          public Frame apply(Payload payload) {
                                            final Frame requestFrame;
                                            if (firstPayload.compareAndSet(true, false)) {
                                              requestFrame =
                                                  Frame.Request.from(
                                                      streamId, requestType, payload, l);
                                            } else {
                                              requestFrame =
                                                  Frame.PayloadFrame.from(
                                                      streamId, FrameType.NEXT, payload);
                                            }
                                            return requestFrame;
                                          }
                                        })
                                    .doOnComplete(
                                        () -> {
                                          if (FrameType.REQUEST_CHANNEL == requestType) {
                                            sendOneFrame(
                                                Frame.PayloadFrame.from(
                                                    streamId, FrameType.COMPLETE));
                                            if (firstPayload.get()) {
                                              receiver.onComplete();
                                            }
                                          }
                                        });

                            requestFrames.subscribe(
                                sendProcessor::onNext,
                                t -> {
                                  errorConsumer.accept(t);
                                  receiver.dispose();
                                });
                          } else {
                            sendOneFrame(Frame.RequestN.from(streamId, l));
                          }
                        })
                    .doOnError(t -> sendOneFrame(Frame.Error.from(streamId, t)))
                    .doOnCancel(
                        () -> {
                          sendOneFrame(Frame.Cancel.from(streamId));
                          if (subscribedRequests != null) {
                            subscribedRequests.cancel();
                          }
                        })
                    .doFinally(
                        s -> {
                          receivers.remove(streamId);
                          senders.remove(streamId);
                        });
              }
            }));
  }

  private boolean contains(int streamId) {
    return receivers.containsKey(streamId);
  }

  protected void cleanup() {
    closedError = CLOSED_CHANNEL_EXCEPTION;
    try {
      for (UnicastProcessor<Payload> subscriber : receivers.values()) {
        cleanUpSubscriber(subscriber);
      }
      for (LimitableRequestPublisher p : senders.values()) {
        cleanUpLimitableRequestPublisher(p);
      }
    } finally {
      senders.clear();
      receivers.clear();
    }
  }

  private synchronized void cleanUpLimitableRequestPublisher(
      LimitableRequestPublisher<?> limitableRequestPublisher) {
    try {
      limitableRequestPublisher.cancel();
    } catch (Throwable t) {
      errorConsumer.accept(t);
    }
  }

  private synchronized void cleanUpSubscriber(UnicastProcessor<?> subscriber) {
    try {
      subscriber.cancel();
    } catch (Throwable t) {
      errorConsumer.accept(t);
    }
  }

  private void handleIncomingFrames(Frame frame) {
    try {
      handleFrame(frame);
    } finally {
      frame.release();
    }
  }

  private void handleFrame(Frame frame) {
    int streamId = frame.getStreamId();
    FrameType type = frame.getType();
    Subscriber<Payload> receiver = receivers.get(streamId);
    if (receiver == null) {
      handleMissingResponseProcessor(streamId, type, frame);
    } else {
      switch (type) {
        case ERROR:
          receiver.onError(Exceptions.from(frame));
          receivers.remove(streamId);
          break;
        case NEXT_COMPLETE:
          receiver.onNext(new PayloadImpl(frame));
          receiver.onComplete();
          break;
        case CANCEL:
          {
            LimitableRequestPublisher sender = senders.remove(streamId);
            receivers.remove(streamId);
            if (sender != null) {
              sender.cancel();
            }
            break;
          }
        case NEXT:
          receiver.onNext(new PayloadImpl(frame));
          break;
        case REQUEST_N:
          {
            LimitableRequestPublisher sender = senders.get(streamId);
            if (sender != null) {
              int n = Frame.RequestN.requestN(frame);
              sender.increaseRequestLimit(n);
              sendProcessor.drain();
            }
            break;
          }
        case COMPLETE:
          receiver.onComplete();
          receivers.remove(streamId);
          break;
        default:
          throw new IllegalStateException(
              "Client received supported frame on stream " + streamId + ": " + frame.toString());
      }
    }
  }

  private void handleMissingResponseProcessor(int streamId, FrameType type, Frame frame) {
    if (!streamIdSupplier.isBeforeOrCurrent(streamId)) {
      if (type == FrameType.ERROR) {
        // message for stream that has never existed, we have a problem with
        // the overall connection and must tear down
        String errorMessage = frame.getDataUtf8();

        throw new IllegalStateException(
            "Client received error for non-existent stream: "
                + streamId
                + " Message: "
                + errorMessage);
      } else {
        throw new IllegalStateException(
            "Client received message for non-existent stream: "
                + streamId
                + ", frame type: "
                + type);
      }
    }
    // receiving a frame after a given stream has been cancelled/completed,
    // so ignore (cancellation is async so there is a race condition)
  }
}
