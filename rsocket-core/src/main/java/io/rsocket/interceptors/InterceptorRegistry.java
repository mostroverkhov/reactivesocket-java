/*
 * Copyright 2016 Netflix, Inc.
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

package io.rsocket.interceptors;

import io.rsocket.DuplexConnection;
import io.rsocket.RSocket;
import java.util.ArrayList;
import java.util.List;

public class InterceptorRegistry {
  private final List<DuplexConnectionInterceptor> connections = new ArrayList<>();
  private final List<RSocketInterceptor> requesters = new ArrayList<>();
  private final List<RSocketInterceptor> handlers = new ArrayList<>();

  public InterceptorRegistry() {}

  public void addConnectionInterceptor(DuplexConnectionInterceptor interceptor) {
    connections.add(interceptor);
  }

  public void addRequesterInterceptor(RSocketInterceptor interceptor) {
    requesters.add(interceptor);
  }

  public void addHandlerInterceptor(RSocketInterceptor interceptor) {
    handlers.add(interceptor);
  }

  public RSocket interceptRequester(RSocket rSocket) {
    for (RSocketInterceptor i : requesters) {
      rSocket = i.apply(rSocket);
    }

    return rSocket;
  }

  public RSocket interceptHandler(RSocket rSocket) {
    for (RSocketInterceptor i : handlers) {
      rSocket = i.apply(rSocket);
    }
    return rSocket;
  }

  public DuplexConnection interceptConnection(
      DuplexConnectionInterceptor.Type type, DuplexConnection connection) {
    for (DuplexConnectionInterceptor i : connections) {
      connection = i.apply(type, connection);
    }
    return connection;
  }
}
