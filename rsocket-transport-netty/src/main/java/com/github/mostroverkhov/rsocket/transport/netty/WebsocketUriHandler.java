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

package com.github.mostroverkhov.rsocket.transport.netty;

import com.github.mostroverkhov.rsocket.transport.ClientTransport;
import com.github.mostroverkhov.rsocket.transport.ServerTransport;
import com.github.mostroverkhov.rsocket.transport.netty.client.WebsocketClientTransport;
import com.github.mostroverkhov.rsocket.transport.netty.server.WebsocketServerTransport;
import com.github.mostroverkhov.rsocket.uri.UriHandler;
import java.net.URI;
import java.util.Optional;

public class WebsocketUriHandler implements UriHandler {
  @Override
  public Optional<ClientTransport> buildClient(URI uri) {
    if ("ws".equals(uri.getScheme()) || "wss".equals(uri.getScheme())) {
      return Optional.of(WebsocketClientTransport.create(uri));
    }

    return UriHandler.super.buildClient(uri);
  }

  @Override
  public Optional<ServerTransport> buildServer(URI uri) {
    if ("ws".equals(uri.getScheme())) {
      return Optional.of(
          WebsocketServerTransport.create(
              uri.getHost(), WebsocketClientTransport.getPort(uri, 80)));
    }

    return UriHandler.super.buildServer(uri);
  }
}
