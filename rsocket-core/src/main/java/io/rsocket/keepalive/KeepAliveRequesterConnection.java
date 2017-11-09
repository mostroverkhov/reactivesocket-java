package io.rsocket.keepalive;

import io.netty.buffer.Unpooled;
import io.rsocket.DuplexConnection;
import io.rsocket.DuplexConnectionProxy;
import io.rsocket.Frame;
import io.rsocket.FrameType;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.rsocket.keepalive.KeepAlive.KeepAliveAvailable;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

import static io.rsocket.keepalive.KeepAlive.*;

public class KeepAliveRequesterConnection extends DuplexConnectionProxy {

  private final FluxProcessor<KeepAliveAvailable, KeepAliveAvailable> keepAliveAvailable =
      UnicastProcessor.<KeepAliveAvailable>create().serialize();

  private final FluxProcessor<KeepAliveMissing, KeepAliveMissing> keepAliveMissing =
          UnicastProcessor.<KeepAliveMissing>create().serialize();

  private final Duration tickPeriod;
  private final int timeoutTicks;
  private final Supplier<ByteBuffer> frameDataFactory;
  private Consumer<Throwable> errConsumer;
  private final long timeoutMillis;
  private volatile long timeLastTickSentMs;
  private final AtomicInteger missedAckCounter = new AtomicInteger();
  private volatile Disposable keepAliveSubs;

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
  }

  @Override
  public Flux<Frame> receive() {
    return super.receive()
        .doOnSubscribe(s -> startPeriodicKeepAlive())
        .doOnNext(this::handleKeepAliveResponse)
        .doOnError(err -> disposeKeepAlive());
  }

  private void handleKeepAliveResponse(Frame f) {
    if (isKeepAliveResponse(f)) {
      missedAckCounter.set(0);
      keepAliveAvailable.onNext(new KeepAliveAvailable(f.getData()));
      timeLastTickSentMs = System.currentTimeMillis();
    }
  }

  public Flux<KeepAliveAvailable> keepAliveAvailable() {
    return keepAliveAvailable;
  }

  public Flux<KeepAliveMissing> keepAliveMissing() {
    return keepAliveMissing;
  }

  private boolean isKeepAliveResponse(Frame f) {
    return f.getType() == FrameType.KEEPALIVE && !Frame.Keepalive.hasRespondFlag(f);
  }

  private void startPeriodicKeepAlive() {
    timeLastTickSentMs = System.currentTimeMillis();
    keepAliveSubs =
        Flux.interval(tickPeriod)
            .concatMap(i -> sendKeepAlive())
            .subscribe(
                __ -> {},
                err -> {
                  complete();
                  errConsumer.accept(err);
                  close().subscribe();
                });
  }

  private void complete() {
    keepAliveAvailable.onComplete();
    keepAliveMissing.onComplete();
  }

  private void disposeKeepAlive() {
    if (keepAliveSubs != null) {
      keepAliveSubs.dispose();
    }
    complete();
  }

  private Mono<Void> sendKeepAlive() {
    long now = System.currentTimeMillis();
    if (now - timeLastTickSentMs > timeoutMillis) {
      int count = missedAckCounter.incrementAndGet();
      if (count >= timeoutTicks) {
        keepAliveMissing.onNext(new KeepAliveMissing(tickPeriod, timeoutTicks, count));
      }
    }
    return sendOne(Frame.Keepalive.from(Unpooled.wrappedBuffer(frameDataFactory.get()), true));
  }
}
