package io.rsocket.interceptors;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class InterceptorFactory {
  private final List<Supplier<Interceptor>> interceptors = new ArrayList<>();

  public InterceptorFactory interceptor(Supplier<Interceptor> interceptor) {
    interceptors.add(interceptor);
    return this;
  }

  public InterceptorRegistry create() {
    InterceptorRegistry registry = new InterceptorRegistry();
    interceptors.forEach(
        interceptorF -> {
          Interceptor interceptor = interceptorF.get();
          interceptor.connInterceptors.forEach(registry::addConnectionInterceptor);
          interceptor.requesterInterceptors.forEach(registry::addRequesterInterceptor);
          interceptor.handlerInterceptors.forEach(registry::addHandlerInterceptor);
        });
    return registry;
  }

  public static class Interceptor {
    private final List<DuplexConnectionInterceptor> connInterceptors = new ArrayList<>();
    private final List<RSocketInterceptor> requesterInterceptors = new ArrayList<>();
    private final List<RSocketInterceptor> handlerInterceptors = new ArrayList<>();

    public Interceptor connection(DuplexConnectionInterceptor interceptor) {
      connInterceptors.add(interceptor);
      return this;
    }

    public Interceptor requesterRSocket(RSocketInterceptor interceptor) {
      requesterInterceptors.add(interceptor);
      return this;
    }

    public Interceptor handlerRSocket(RSocketInterceptor interceptor) {
      handlerInterceptors.add(interceptor);
      return this;
    }
  }
}
