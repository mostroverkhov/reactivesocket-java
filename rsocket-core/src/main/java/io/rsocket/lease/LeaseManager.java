package io.rsocket.lease;

import io.rsocket.exceptions.NoLeaseException;
import javax.annotation.Nonnull;

/** Updates Lease on use and grant */
class LeaseManager {
  private static final LeaseImpl INVALID_MUTABLE_LEASE = new LeaseImpl(0, 0, null);
  private volatile LeaseImpl currentLease = INVALID_MUTABLE_LEASE;
  private final String tag;

  public LeaseManager(@Nonnull String tag) {
    this.tag = tag;
  }

  public double availability() {
    return currentLease.availability();
  }

  public void grantLease(int numberOfRequests, int ttl) {
    assertGrantedLease(numberOfRequests, ttl);
    this.currentLease = new LeaseImpl(numberOfRequests, ttl, null);
  }

  public void useLease() {
    if (currentLease.isValid()) {
      currentLease.use(1);
    } else {
      throw new NoLeaseException(currentLease, tag);
    }
  }

  private static void assertGrantedLease(int numberOfRequests, int ttl) {
    if (numberOfRequests < 0) {
      throw new IllegalArgumentException("numberOfRequests should be non-negative");
    }
    if (ttl < 0) {
      throw new IllegalArgumentException("ttl should be non-negative");
    }
  }

  @Override
  public String toString() {
    return "LeaseManager{" + "tag='" + tag + '\'' + '}';
  }
}
