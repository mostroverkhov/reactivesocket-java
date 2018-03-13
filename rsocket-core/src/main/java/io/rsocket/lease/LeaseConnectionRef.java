package io.rsocket.lease;

import java.nio.ByteBuffer;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import reactor.core.publisher.Mono;

/** Provides means to grant lease to peer */
public class LeaseConnectionRef {
  private final LeaseGranter leaseGranter;
  private final Mono<Void> onClose;

  LeaseConnectionRef(LeaseGranter granter, @Nonnull Mono<Void> onClose) {
    Objects.requireNonNull(granter, "leaseGranter");
    Objects.requireNonNull(onClose, "onClose");
    this.onClose = onClose;
    this.leaseGranter = granter;
  }

  public Mono<Void> grantLease(
      int numberOfRequests, long ttlSeconds, @Nullable ByteBuffer metadata) {
    assertArgs(numberOfRequests, ttlSeconds);
    int ttl = Math.toIntExact(ttlSeconds);
    return leaseGranter.grantLease(numberOfRequests, ttl, metadata);
  }

  public Mono<Void> grantLease(int numberOfRequests, long timeToLive) {
    return grantLease(numberOfRequests, timeToLive, ByteBuffer.allocate(0));
  }

  public Mono<Void> onClose() {
    return onClose;
  }

  private void assertArgs(int numberOfRequests, long ttl) {
    if (numberOfRequests <= 0) {
      throw new IllegalArgumentException("numberOfRequests must be non-negative");
    }

    if (ttl <= 0) {
      throw new IllegalArgumentException("timeToLive must be non-negative");
    }
  }
}
