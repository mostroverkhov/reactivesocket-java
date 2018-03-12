package io.rsocket.lease;

import io.rsocket.DuplexConnection;
import io.rsocket.interceptors.DuplexConnectionInterceptor;

class ServerLeaseEnablingInterceptor implements DuplexConnectionInterceptor {
  private final LeaseContext leaseContext;

  public ServerLeaseEnablingInterceptor(LeaseContext leaseContext) {
    this.leaseContext = leaseContext;
  }

  @Override
  public DuplexConnection apply(Type type, DuplexConnection connection) {
    if (type == Type.INIT) {
      return new ServerLeaseEnablingConnection(connection, leaseContext);
    } else {
      return connection;
    }
  }
}
