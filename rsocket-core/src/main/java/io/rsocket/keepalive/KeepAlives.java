package io.rsocket.keepalive;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.rsocket.keepalive.KeepAlive.*;

public class KeepAlives {
  private final Flux<KeepAliveAvailable> keepAliveAvailable;
  private final Flux<KeepAliveMissing> keepAliveMissing;
  private final Mono<Void> closeConnection;

  public KeepAlives(Flux<KeepAliveAvailable> keepAliveAvailable,
                    Flux<KeepAliveMissing> keepAliveMissing,
                    Mono<Void> closeConnection) {
    this.keepAliveAvailable = keepAliveAvailable;
    this.keepAliveMissing = keepAliveMissing;
    this.closeConnection = closeConnection;
  }

  public Flux<KeepAliveAvailable> keepAliveAvailable() {
    return keepAliveAvailable;
  }

  public Flux<KeepAliveMissing> keepAliveMissing() {
    return keepAliveMissing;
  }

  public Mono<Void> closeConnection() {
    return closeConnection;
  }
}
