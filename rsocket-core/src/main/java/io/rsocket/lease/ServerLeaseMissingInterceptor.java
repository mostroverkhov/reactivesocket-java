package io.rsocket.lease;

import io.rsocket.DuplexConnection;
import io.rsocket.interceptors.DuplexConnectionInterceptor;

class ServerLeaseMissingInterceptor implements DuplexConnectionInterceptor {
  @Override
  public DuplexConnection apply(Type type, DuplexConnection connection) {
    if (type == Type.INIT) {
      return new ServerLeaseMissingConnection(connection);
    } else {
      return connection;
    }
  }
}
