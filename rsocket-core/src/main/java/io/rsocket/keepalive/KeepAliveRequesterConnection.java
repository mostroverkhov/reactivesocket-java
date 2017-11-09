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
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

public class KeepAliveRequesterConnection extends DuplexConnectionProxy {

  private final FluxProcessor<KeepAlive, KeepAlive> keepAlives =
      UnicastProcessor.<KeepAlive>create().serialize();
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
      keepAlives.onNext(new KeepAlive.KeepAliveAvailable(f.getData()));
      timeLastTickSentMs = System.currentTimeMillis();
    }
  }

  public Flux<KeepAlive> keepAlive() {
    return keepAlives;
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
                  errConsumer.accept(err);
                  keepAlives.onComplete();
                  close().subscribe();
                });
  }

  private void disposeKeepAlive() {
    if (keepAliveSubs != null) {
      keepAliveSubs.dispose();
    }
    keepAlives.onComplete();
  }

  private Mono<Void> sendKeepAlive() {
    long now = System.currentTimeMillis();
    if (now - timeLastTickSentMs > timeoutMillis) {
      int count = missedAckCounter.incrementAndGet();
      if (count >= timeoutTicks) {
        // TODO fix race with KeepAlive.KeepAliveAvailable
        keepAlives.onNext(new KeepAlive.KeepAliveMissing(tickPeriod, timeoutTicks, count));
      }
    }
    return sendOne(Frame.Keepalive.from(Unpooled.wrappedBuffer(frameDataFactory.get()), true));
  }
}
