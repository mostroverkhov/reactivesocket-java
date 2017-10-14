package io.rsocket.lease;

import java.nio.ByteBuffer;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Provides means to grant lease to peer and check RSocket responder availability */
public class LeaseConnectionRef {
  private final LeaseGranter leaseGranter;
  private volatile Mono<Void> onClose;

  LeaseConnectionRef(@Nonnull LeaseGranter leaseGranter, @Nonnull Mono<Void> onClose) {
    Objects.requireNonNull(leaseGranter, "leaseGranter");
    Objects.requireNonNull(onClose, "onClose");
    this.onClose = onClose;
    this.leaseGranter = leaseGranter;
  }

  public void grantLease(int numberOfRequests, long timeToLive, @Nullable ByteBuffer metadata) {
    assertArgs(numberOfRequests, timeToLive);
    leaseGranter.grantLease(numberOfRequests, Math.toIntExact(timeToLive), metadata);
  }

  public void grantLease(int numberOfRequests, long timeToLive) {
    grantLease(numberOfRequests, timeToLive, ByteBuffer.allocate(0));
  }

  public void withdrawLease() {
    grantLease(0, 0);
  }

  public Flux<Lease> inboundLease() {
    return leaseGranter.inboundLease();
  }

  public Mono<Void> onClose() {
    return onClose;
  }

  private void assertArgs(int numberOfRequests, long ttl) {
    if (numberOfRequests < 0) {
      throw new IllegalArgumentException("numberOfRequests should be non-negative");
    }

    if (ttl < 0) {
      throw new IllegalArgumentException("timeToLive should be non-negative");
    }
  }
}
