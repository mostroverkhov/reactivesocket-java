package io.rsocket.exceptions;

import io.rsocket.lease.Lease;
import javax.annotation.Nonnull;

public class NoLeaseException extends RejectedException {

  private final String tag;

  public NoLeaseException(@Nonnull Lease lease, String tag) {
    super(leaseMessage(lease,tag));
    this.tag = tag;
  }

  static String leaseMessage(Lease lease,String tag) {
    boolean expired = lease.isExpired();
    int allowedRequests = lease.getAllowedRequests();
    return String.format(
        "%s Missing lease. Expired: %b, allowedRequests: %d", tag,expired, allowedRequests);
  }
}
