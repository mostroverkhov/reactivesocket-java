package io.rsocket.lease;

import io.rsocket.DuplexConnection;
import io.rsocket.interceptors.DuplexConnectionInterceptor;
import java.util.function.Consumer;

class LeaseGranterInterceptor implements DuplexConnectionInterceptor {
  private final LeaseManager sender;
  private final LeaseManager receiver;
  private final Consumer<LeaseConnectionRef> leaseHandle;

  public LeaseGranterInterceptor(
      LeaseManager sender, LeaseManager receiver, Consumer<LeaseConnectionRef> leaseHandle) {
    this.sender = sender;
    this.receiver = receiver;
    this.leaseHandle = leaseHandle;
  }

  @Override
  public DuplexConnection apply(Type type, DuplexConnection connection) {
    if (type == Type.STREAM_ZERO) {
      LeaseConnection leaseConnection = new LeaseConnection(connection, sender, receiver);
      leaseHandle.accept(new LeaseConnectionRef(leaseConnection, leaseConnection.onClose()));
      return leaseConnection;
    } else {
      return connection;
    }
  }
}
