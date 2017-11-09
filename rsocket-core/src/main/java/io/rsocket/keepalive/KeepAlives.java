package io.rsocket.keepalive;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class KeepAlives {
  private Flux<KeepAlive> keepAlive;
  private Mono<Void> closeConnection;

  public KeepAlives(Flux<KeepAlive> keepAlive, Mono<Void> closeConnection) {
    this.keepAlive = keepAlive;
    this.closeConnection = closeConnection;
  }

  public Flux<KeepAlive> keepAlive() {
    return keepAlive;
  }

  public Mono<Void> closeConnection() {
    return closeConnection;
  }
}
