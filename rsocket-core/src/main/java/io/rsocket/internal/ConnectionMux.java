package io.rsocket.internal;

import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ConnectionMux implements DuplexConnection {
  private final DuplexConnection sender;
  private final List<DuplexConnection> demuxed;

  public ConnectionMux(DuplexConnection sender, DuplexConnection... demuxed) {
    this.sender = sender;
    this.demuxed = Arrays.asList(demuxed);
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frame) {
    return sender.send(frame);
  }

  @Override
  public Flux<Frame> receive() {
    return Flux.merge(transform(demuxed, DuplexConnection::receive));
  }

  @Override
  public double availability() {
    return sender.availability();
  }

  @Override
  public Mono<Void> close() {
    return whenCompleted(DuplexConnection::close);
  }

  @Override
  public Mono<Void> onClose() {
    return whenCompleted(DuplexConnection::onClose);
  }

  Mono<Void> whenCompleted(Function<DuplexConnection, Mono<Void>> mapper) {
    return Flux.fromIterable(demuxed).flatMap(mapper).then();
  }

  static <T> List<T> transform(List<DuplexConnection> col, Function<DuplexConnection, T> mapper) {
    return col.stream().map(mapper).collect(toList());
  }
}
