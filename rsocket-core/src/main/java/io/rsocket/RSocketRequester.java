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

package io.rsocket;

import static io.rsocket.util.ExceptionUtil.noStacktrace;

import io.netty.util.collection.IntObjectHashMap;
import io.rsocket.exceptions.Exceptions;
import io.rsocket.internal.LimitableRequestPublisher;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.util.PayloadImpl;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.*;

/** Client Side of a RSocket socket. Sends {@link Frame}s to a {@link RSocketResponder} */
class RSocketRequester implements RSocket {

  private static final ClosedChannelException CLOSED_CHANNEL_EXCEPTION =
      noStacktrace(new ClosedChannelException());

  private final DuplexConnection connection;
  private final Consumer<Throwable> errorConsumer;
  private final StreamIdSupplier streamIdSupplier;
  private final MonoProcessor<Void> started;
  private final IntObjectHashMap<LimitableRequestPublisher> senders;
  private final IntObjectHashMap<Subscriber<Payload>> receivers;
  private final UnboundedProcessor<Send> sendProcessor;
  private volatile Throwable errorSignal;

  RSocketRequester(
      DuplexConnection connection,
      Consumer<Throwable> errorConsumer,
      StreamIdSupplier streamIdSupplier) {
    this.connection = connection;
    this.errorConsumer = errorConsumer;
    this.streamIdSupplier = streamIdSupplier;
    this.started = MonoProcessor.create();
    this.senders = new IntObjectHashMap<>(256, 0.9f);
    this.receivers = new IntObjectHashMap<>(256, 0.9f);

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    this.sendProcessor = new UnboundedProcessor<>();

    connection
        .onClose()
        .doFinally(
            signalType -> {
              cleanup();
            })
        .doOnError(errorConsumer)
        .subscribe();

    send(sendProcessor)
        .doOnError(this::handleSendProcessorError)
        .doFinally(this::handleSendProcessorCancel)
        .subscribe();

    connection
        .receive()
        .doOnSubscribe(subscription -> started.onComplete())
        .doOnNext(this::handleIncomingFrames)
        .doOnError(errorConsumer)
        .subscribe();
  }

  private void handleSendProcessorError(Throwable t) {
    Collection<Subscriber<Payload>> values;
    Collection<LimitableRequestPublisher> values1;
    synchronized (RSocketRequester.this) {
      values = receivers.values();
      values1 = senders.values();
    }

    for (Subscriber subscriber : values) {
      try {
        subscriber.onError(t);
      } catch (Throwable e) {
        errorConsumer.accept(e);
      }
    }

    for (LimitableRequestPublisher p : values1) {
      p.cancel();
    }
  }

  private Mono<Void> send(Flux<Send> sends) {
    return sends.flatMap(Send::send).then();
  }

  private void handleSendProcessorCancel(SignalType t) {
    if (SignalType.ON_ERROR == t) {
      return;
    }
    Collection<Subscriber<Payload>> values;
    Collection<LimitableRequestPublisher> values1;
    synchronized (RSocketRequester.this) {
      values = receivers.values();
      values1 = senders.values();
    }

    for (Subscriber subscriber : values) {
      try {
        subscriber.onError(new Throwable("closed connection"));
      } catch (Throwable e) {
        errorConsumer.accept(e);
      }
    }

    for (LimitableRequestPublisher p : values1) {
      p.cancel();
    }
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return errorOrPublisher(() -> handleFireAndForget(payload), Mono::error);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return errorOrPublisher(() -> handleRequestResponse(payload), Mono::error);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return errorOrPublisher(() -> handleRequestStream(payload), Flux::error);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return errorOrPublisher(
        () -> handleChannel(Flux.from(payloads), FrameType.REQUEST_CHANNEL), Flux::error);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return errorOrPublisher(() -> handleMetadataPush(payload), Mono::error);
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

  private <T, K extends Publisher<T>> K errorOrPublisher(
      Supplier<K> pubSupplier, Function<Throwable, K> errF) {
    return errorSignal != null ? errF.apply(errorSignal) : pubSupplier.get();
  }

  private Mono<Void> handleMetadataPush(Payload payload) {
    final Frame requestFrame = Frame.Request.from(0, FrameType.METADATA_PUSH, payload, 1);
    sendProcessor.onNext(new Send(requestFrame));
    return Mono.empty();
  }

  private Mono<Void> handleFireAndForget(Payload payload) {
    return started.then(
        Mono.fromRunnable(
            () -> {
              final int streamId = streamIdSupplier.nextStreamId();
              final Frame requestFrame =
                  Frame.Request.from(streamId, FrameType.FIRE_AND_FORGET, payload, 1);
              sendProcessor.onNext(new Send(requestFrame));
            }));
  }

  private Flux<Payload> handleRequestStream(final Payload payload) {
    return started.thenMany(
        Flux.defer(
            () -> {
              int streamId = streamIdSupplier.nextStreamId();

              UnicastProcessor<Payload> receiver = UnicastProcessor.create();

              synchronized (this) {
                receivers.put(streamId, receiver);
              }

              AtomicBoolean first = new AtomicBoolean(false);

              return receiver
                  .doOnRequest(
                      l -> {
                        if (first.compareAndSet(false, true) && !receiver.isTerminated()) {
                          final Frame requestFrame =
                              Frame.Request.from(streamId, FrameType.REQUEST_STREAM, payload, l);

                          sendProcessor.onNext(new Send<>(requestFrame, receiver));
                        } else if (contains(streamId) && !receiver.isTerminated()) {
                          sendProcessor.onNext(
                              new Send<>(Frame.RequestN.from(streamId, l), receiver));
                        }
                        sendProcessor.drain();
                      })
                  .doOnError(
                      t -> {
                        if (contains(streamId) && !receiver.isTerminated()) {
                          sendProcessor.onNext(new Send<>(Frame.Error.from(streamId, t), receiver));
                        }
                      })
                  .doOnCancel(
                      () -> {
                        if (contains(streamId) && !receiver.isTerminated()) {
                          sendProcessor.onNext(new Send<>(Frame.Cancel.from(streamId), receiver));
                        }
                      })
                  .doFinally(
                      s -> {
                        removeReceiver(streamId);
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

              MonoProcessor<Payload> receiver = MonoProcessor.create();

              synchronized (this) {
                receivers.put(streamId, receiver);
              }

              sendProcessor.onNext(new Send<>(requestFrame, receiver));

              return receiver
                  .doOnError(
                      t ->
                          sendProcessor.onNext(new Send<>(Frame.Error.from(streamId, t), receiver)))
                  .doOnCancel(
                      () -> sendProcessor.onNext(new Send<>(Frame.Cancel.from(streamId), receiver)))
                  .doFinally(
                      s -> {
                        removeReceiver(streamId);
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
                return contains(streamId) && !receiver.isTerminated();
              }

              void sendOneFrame(Frame frame) {
                if (isValidToSendFrame()) {
                  sendProcessor.onNext(new Send<>(frame, receiver));
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
                                          synchronized (RSocketRequester.this) {
                                            senders.put(streamId, wrapped);
                                            receivers.put(streamId, receiver);
                                          }

                                          return wrapped;
                                        })
                                    .map(
                                        new Function<Payload, Frame>() {

                                          @Override
                                          public Frame apply(Payload payload) {
                                            if (firstPayload.compareAndSet(true, false)) {
                                              return Frame.Request.from(
                                                  streamId, requestType, payload, l);
                                            } else {
                                              return Frame.PayloadFrame.from(
                                                  streamId, FrameType.NEXT, payload);
                                            }
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

                            requestFrames
                                .doOnNext(t1 -> sendProcessor.onNext(new Send<>(t1, receiver)))
                                .doOnError(
                                    t -> {
                                      errorConsumer.accept(t);
                                      receiver.cancel();
                                    })
                                .subscribe();
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
                          removeReceiver(streamId);
                          removeSender(streamId);
                        });
              }
            }));
  }

  private boolean contains(int streamId) {
    synchronized (RSocketRequester.this) {
      return receivers.containsKey(streamId);
    }
  }

  protected void cleanup() {
    errorSignal = CLOSED_CHANNEL_EXCEPTION;
    try {
      Collection<Subscriber<Payload>> subscribers;
      Collection<LimitableRequestPublisher> publishers;
      synchronized (RSocketRequester.this) {
        subscribers = receivers.values();
        publishers = senders.values();
      }
      subscribers.forEach(this::cleanUpSubscriber);
      publishers.forEach(this::cleanUpLimitableRequestPublisher);
    } finally {
      synchronized (this) {
        senders.clear();
        receivers.clear();
      }
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

  private synchronized void cleanUpSubscriber(Subscriber<?> subscriber) {
    try {
      subscriber.onError(CLOSED_CHANNEL_EXCEPTION);
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
    Subscriber<Payload> receiver;
    synchronized (this) {
      receiver = receivers.get(streamId);
    }
    if (receiver == null) {
      handleMissingResponseProcessor(streamId, type, frame);
    } else {
      switch (type) {
        case ERROR:
          receiver.onError(Exceptions.from(frame));
          removeReceiver(streamId);
          break;
        case NEXT_COMPLETE:
          receiver.onNext(new PayloadImpl(frame));
          receiver.onComplete();
          break;
        case CANCEL:
          {
            LimitableRequestPublisher sender;
            synchronized (this) {
              sender = senders.remove(streamId);
              removeReceiver(streamId);
            }
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
            LimitableRequestPublisher sender;
            synchronized (this) {
              sender = senders.get(streamId);
            }
            if (sender != null) {
              int n = Frame.RequestN.requestN(frame);
              sender.increaseRequestLimit(n);
              sendProcessor.drain();
            }
            break;
          }
        case COMPLETE:
          receiver.onComplete();
          synchronized (this) {
            receivers.remove(streamId);
          }
          break;
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

  private synchronized void removeReceiver(int streamId) {
    receivers.remove(streamId);
  }

  private synchronized void removeSender(int streamId) {
    senders.remove(streamId);
  }

  private class Send<T extends Subscriber<?> & Disposable> {
    private final Frame frame;
    private final T subscriber;

    Send(Frame frame, T subscriber) {
      this.frame = frame;
      this.subscriber = subscriber;
    }

    public Send(Frame frame) {
      this(frame, null);
    }

    public Mono<Void> send() {
      return connection
          .send(Mono.just(frame))
          .onErrorResume(
              err -> {
                if (err instanceof ClosedChannelException) {
                  return Mono.error(err);
                } else {
                  if (subscriber != null && !subscriber.isDisposed()) {
                    subscriber.onError(err);
                  }
                  return Mono.empty();
                }
              })
          .then();
    }
  }
}
