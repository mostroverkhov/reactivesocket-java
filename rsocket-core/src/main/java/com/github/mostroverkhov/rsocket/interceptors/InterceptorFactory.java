/*
 * Copyright 2018 Maksym Ostroverkhov
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.github.mostroverkhov.rsocket.interceptors;

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

  public InterceptorFactory tailInterceptorSet(Supplier<InterceptorSet> interceptor) {
    interceptors.add(interceptor);
    return this;
  }

  public InterceptorFactory headInterceptorSet(Supplier<InterceptorSet> interceptor) {
    interceptors.add(0, interceptor);
    return this;
  }

  public InterceptorFactory addConnectionInterceptor(DuplexConnectionInterceptor interceptor) {
    tailInterceptorSet(() -> new InterceptorSet().connection(interceptor));
    return this;
  }

  public InterceptorFactory addRequesterInterceptor(RSocketInterceptor interceptor) {
    tailInterceptorSet(() -> new InterceptorSet().requesterRSocket(interceptor));
    return this;
  }

  public InterceptorFactory addHandlerInterceptor(RSocketInterceptor interceptor) {
    tailInterceptorSet(() -> new InterceptorSet().handlerRSocket(interceptor));
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
