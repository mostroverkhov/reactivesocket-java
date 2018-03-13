package io.rsocket.keepalive;

import io.rsocket.DuplexConnection;
import io.rsocket.interceptors.DuplexConnectionInterceptor;

public class KeepAliveResponderInterceptor implements DuplexConnectionInterceptor {
  @Override
  public DuplexConnection apply(Type type, DuplexConnection connection) {
    if (type == Type.STREAM_ZERO) {
      return new KeepAliveResponderConnection(connection);
    } else {
      return connection;
    }
  }
}
