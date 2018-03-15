package io.rsocket.keepalive;

import java.nio.ByteBuffer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class KeepAlives {
  private final Flux<ByteBuffer> keepAliveAvailable;
  private final Flux<KeepAliveMissing> keepAliveMissing;
  private final Mono<Void> closeConnection;

  public KeepAlives(
      Flux<ByteBuffer> keepAliveAvailable,
      Flux<KeepAliveMissing> keepAliveMissing,
      Mono<Void> closeConnection) {
    this.keepAliveAvailable = keepAliveAvailable;
    this.keepAliveMissing = keepAliveMissing;
    this.closeConnection = closeConnection;
  }

  public Flux<ByteBuffer> keepAlive() {
    return keepAliveAvailable;
  }

  public Flux<KeepAliveMissing> keepAliveMissing() {
    return keepAliveMissing;
  }

  public Mono<Void> closeConnection() {
    return closeConnection;
  }
}
