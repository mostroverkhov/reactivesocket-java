package io.rsocket.lease;

import io.rsocket.RSocket;
import io.rsocket.interceptors.RSocketInterceptor;

class LeaseInterceptor implements RSocketInterceptor {

  private final String tag;
  private final LeaseManager leaseManager;
  private final LeaseContext leaseContext;

  public LeaseInterceptor(LeaseContext leaseContext, String tag, LeaseManager leaseManager) {
    this.leaseContext = leaseContext;
    this.tag = tag;
    this.leaseManager = leaseManager;
  }

  @Override
  public RSocket apply(RSocket rSocket) {
    return new LeaseRSocket(leaseContext, rSocket, tag, leaseManager);
  }
}
