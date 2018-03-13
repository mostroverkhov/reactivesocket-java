package io.rsocket.interceptors;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class InterceptorFactory {
  private final List<Supplier<InterceptorSet>> interceptors = new ArrayList<>();

  public InterceptorFactory() {}

  private InterceptorFactory(InterceptorFactory interceptorFactory) {
    interceptors.addAll(interceptorFactory.interceptors);
  }

  public InterceptorFactory copy() {
    return new InterceptorFactory(this);
  }

  public InterceptorFactory addInterceptorSet(Supplier<InterceptorSet> interceptor) {
    interceptors.add(interceptor);
    return this;
  }

  public InterceptorFactory addConnectionInterceptor(DuplexConnectionInterceptor interceptor) {
    addInterceptorSet(() -> new InterceptorSet().connection(interceptor));
    return this;
  }

  public InterceptorFactory addRequesterInterceptor(RSocketInterceptor interceptor) {
    addInterceptorSet(() -> new InterceptorSet().requesterRSocket(interceptor));
    return this;
  }

  public InterceptorFactory addHandlerInterceptor(RSocketInterceptor interceptor) {
    addInterceptorSet(() -> new InterceptorSet().handlerRSocket(interceptor));
    return this;
  }

  public InterceptorRegistry create() {
    InterceptorRegistry registry = new InterceptorRegistry();
    interceptors.forEach(
        interceptorF -> {
          InterceptorSet interceptorSet = interceptorF.get();
          interceptorSet.connInterceptors.forEach(registry::addConnectionInterceptor);
          interceptorSet.requesterInterceptors.forEach(registry::addRequesterInterceptor);
          interceptorSet.handlerInterceptors.forEach(registry::addHandlerInterceptor);
        });
    return registry;
  }

  public static class InterceptorSet {
    private final List<DuplexConnectionInterceptor> connInterceptors = new ArrayList<>();
    private final List<RSocketInterceptor> requesterInterceptors = new ArrayList<>();
    private final List<RSocketInterceptor> handlerInterceptors = new ArrayList<>();

    public InterceptorSet connection(DuplexConnectionInterceptor interceptor) {
      connInterceptors.add(interceptor);
      return this;
    }

    public InterceptorSet requesterRSocket(RSocketInterceptor interceptor) {
      requesterInterceptors.add(interceptor);
      return this;
    }

    public InterceptorSet handlerRSocket(RSocketInterceptor interceptor) {
      handlerInterceptors.add(interceptor);
      return this;
    }
  }
}
