package io.rsocket.plugins;

import io.rsocket.DuplexConnection;
import java.util.function.Function;

public class PerTypeDuplexConnectionInterceptor implements DuplexConnectionInterceptor {
  private final Type type;
  private final Function<DuplexConnection, DuplexConnection> interceptor;

  public PerTypeDuplexConnectionInterceptor(
      Type type, Function<DuplexConnection, DuplexConnection> interceptor) {
    this.type = type;
    this.interceptor = interceptor;
  }

  @Override
  public DuplexConnection apply(Type type, DuplexConnection duplexConnection) {
    if (this.type.equals(type)) {
      duplexConnection = interceptor.apply(duplexConnection);
    }
    return duplexConnection;
  }
}
