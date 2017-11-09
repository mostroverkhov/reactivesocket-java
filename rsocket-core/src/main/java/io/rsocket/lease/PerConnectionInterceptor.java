package io.rsocket.lease;

import static java.util.stream.Collectors.toList;

import io.rsocket.DuplexConnection;
import io.rsocket.internal.ConnectionDemux;
import io.rsocket.internal.ConnectionMux;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.plugins.PluginRegistry;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public class PerConnectionInterceptor implements DuplexConnectionInterceptor {
  private static final PluginRegistry empty = new PluginRegistry();
  private final List<Supplier<DuplexConnectionInterceptor>> interceptorsFactory;

  public PerConnectionInterceptor(Supplier<DuplexConnectionInterceptor>... interceptorsFactory) {
    this.interceptorsFactory = Arrays.asList(interceptorsFactory);
  }

  @Override
  public DuplexConnection apply(Type type, DuplexConnection duplexConnection) {
    if (type.equals(Type.SOURCE)) {
      List<DuplexConnectionInterceptor> interceptors = create();
      duplexConnection = intercept(duplexConnection, Type.SOURCE, interceptors);

      ConnectionDemux demux = new ConnectionDemux(duplexConnection, empty);
      DuplexConnection init = intercept(demux.asInitConnection(), Type.INIT, interceptors);
      DuplexConnection zero =
          intercept(demux.asStreamZeroConnection(), Type.STREAM_ZERO, interceptors);
      DuplexConnection client = intercept(demux.asClientConnection(), Type.CLIENT, interceptors);
      DuplexConnection server = intercept(demux.asServerConnection(), Type.SERVER, interceptors);

      return new ConnectionMux(duplexConnection, init, zero, client, server);
    } else {
      return duplexConnection;
    }
  }

  List<DuplexConnectionInterceptor> create() {
    return interceptorsFactory.stream().map(Supplier::get).collect(toList());
  }

  static DuplexConnection intercept(
      DuplexConnection source, Type type, List<DuplexConnectionInterceptor> interceptors) {
    for (DuplexConnectionInterceptor interceptor : interceptors) {
      source = interceptor.apply(type, source);
    }
    return source;
  }
}
