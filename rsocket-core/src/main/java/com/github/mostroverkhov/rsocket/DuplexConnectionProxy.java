package com.github.mostroverkhov.rsocket;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DuplexConnectionProxy implements DuplexConnection {
  private final DuplexConnection source;

  public DuplexConnectionProxy(DuplexConnection source) {
    this.source = source;
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frame) {
    return source.send(frame);
  }

  @Override
  public Flux<Frame> receive() {
    return source.receive();
  }

  @Override
  public double availability() {
    return source.availability();
  }

  @Override
  public Mono<Void> close() {
    return source.close();
  }

  @Override
  public Mono<Void> onClose() {
    return source.onClose();
  }
}
