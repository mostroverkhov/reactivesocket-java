/*
 * Copyright 2018 Maksym Ostroverkhov
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.github.mostroverkhov.rsocket.keepalive;

import com.github.mostroverkhov.rsocket.DuplexConnection;
import com.github.mostroverkhov.rsocket.DuplexConnectionProxy;
import com.github.mostroverkhov.rsocket.Frame;
import com.github.mostroverkhov.rsocket.FrameType;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

public class KeepAliveRequesterConnection extends DuplexConnectionProxy {

  private final FluxProcessor<ByteBuffer, ByteBuffer> availableEvents =
      UnicastProcessor.<ByteBuffer>create().serialize();

  private final FluxProcessor<KeepAliveMissing, KeepAliveMissing> missingEvents =
      UnicastProcessor.<KeepAliveMissing>create().serialize();

  private final Duration tickPeriod;
  private final int timeoutTicks;
  private final Supplier<ByteBuffer> frameDataFactory;
  private final KeepAliveMissing keepAliveMissing;
  private Consumer<Throwable> errConsumer;
  private final long timeoutMillis;
  private volatile long timeLastTickReceived;
  private final AtomicInteger missedAckCounter = new AtomicInteger();
  private volatile Disposable keepAliveSubs;
  private final AtomicBoolean missingSent = new AtomicBoolean();

  public KeepAliveRequesterConnection(
      DuplexConnection zero,
      Duration tickPeriod,
      int timeoutTicks,
      Supplier<ByteBuffer> frameDataFactory,
      Consumer<Throwable> errConsumer) {
    super(zero);
    this.tickPeriod = tickPeriod;
    this.timeoutTicks = timeoutTicks;
    this.frameDataFactory = frameDataFactory;
    this.errConsumer = errConsumer;
    this.timeoutMillis = tickPeriod.toMillis() * timeoutTicks;
    this.keepAliveMissing = new KeepAliveMissing(tickPeriod, timeoutTicks);
  }

  @Override
  public Flux<Frame> receive() {
    return super.receive()
        .doOnSubscribe(s -> startPeriodicKeepAlive())
        .doOnNext(this::handleKeepAliveAvailable)
        .doOnError(err -> disposeKeepAlive());
  }

  public Flux<ByteBuffer> keepAliveAvailable() {
    return availableEvents;
  }

  public Flux<KeepAliveMissing> keepAliveMissing() {
    return missingEvents;
  }

  private boolean isKeepAliveResponse(Frame f) {
    return f.getType() == FrameType.KEEPALIVE && !Frame.Keepalive.hasRespondFlag(f);
  }

  private void startPeriodicKeepAlive() {
    timeLastTickReceived = System.currentTimeMillis();
    keepAliveSubs =
        Flux.interval(tickPeriod)
            .concatMap(i -> sendAndCheckKeepAlive())
            .subscribe(
                __ -> {},
                err -> {
                  complete();
                  errConsumer.accept(err);
                  close().subscribe();
                });
  }

  private void complete() {
    availableEvents.onComplete();
    missingEvents.onComplete();
  }

  private void disposeKeepAlive() {
    if (keepAliveSubs != null) {
      keepAliveSubs.dispose();
    }
    complete();
  }

  private Mono<Void> sendAndCheckKeepAlive() {
    checkKeepAliveMissing();
    return sendOne(Frame.Keepalive.from(Unpooled.wrappedBuffer(frameDataFactory.get()), true));
  }

  private void checkKeepAliveMissing() {
    long now = System.currentTimeMillis();
    if (now - timeLastTickReceived > timeoutMillis) {
      if (missedAckCounter.incrementAndGet() >= timeoutTicks) {
        if (missingSent.compareAndSet(false, true)) {
          missingEvents.onNext(keepAliveMissing);
        }
      }
    }
  }

  private void handleKeepAliveAvailable(Frame f) {
    if (isKeepAliveResponse(f)) {
      missedAckCounter.set(0);
      availableEvents.onNext(f.getData());
      timeLastTickReceived = System.currentTimeMillis();
      missingSent.set(false);
    }
  }
}
