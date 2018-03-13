package io.rsocket.internal;

import io.rsocket.DuplexConnection;
import io.rsocket.interceptors.DuplexConnectionInterceptor;

public class ConnectionErrorInterceptor implements DuplexConnectionInterceptor {
  @Override
  public DuplexConnection apply(Type type, DuplexConnection connection) {
    if (type == Type.STREAM_ZERO) {
      return new ZeroErrorHandlingConnection(connection);
    } else {
      return connection;
    }
  }
}
