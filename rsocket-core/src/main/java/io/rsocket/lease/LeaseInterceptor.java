package io.rsocket.lease;

import io.rsocket.RSocket;
import io.rsocket.interceptors.RSocketInterceptor;

class LeaseInterceptor implements RSocketInterceptor {

  private final String tag;
  private final LeaseManager leaseManager;

  public LeaseInterceptor(String tag, LeaseManager leaseManager) {
    this.tag = tag;
    this.leaseManager = leaseManager;
  }

  @Override
  public RSocket apply(RSocket rSocket) {
    return new LeaseRSocket(rSocket, tag, leaseManager);
  }
}
