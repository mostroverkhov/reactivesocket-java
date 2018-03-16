/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.mostroverkhov.rsocket.transport.netty;

import com.github.mostroverkhov.rsocket.test.ClientSetupRule;
import com.github.mostroverkhov.rsocket.transport.netty.client.WebsocketClientTransport;
import com.github.mostroverkhov.rsocket.transport.netty.server.NettyContextCloseable;
import com.github.mostroverkhov.rsocket.transport.netty.server.WebsocketServerTransport;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.net.InetSocketAddress;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.http.server.HttpServer;

public class SecureWebsocketClientSetupRule
    extends ClientSetupRule<InetSocketAddress, NettyContextCloseable> {

  public SecureWebsocketClientSetupRule() {
    super(
        () -> new InetSocketAddress("localhost", 0),
        (address, server) ->
            WebsocketClientTransport.create(
                HttpClient.create(
                    options ->
                        options
                            .connectAddress(server::address)
                            .sslSupport(c -> c.trustManager(InsecureTrustManagerFactory.INSTANCE))),
                "https://"
                    + server.address().getHostName()
                    + ":"
                    + server.address().getPort()
                    + "/"),
        address ->
            WebsocketServerTransport.create(
                HttpServer.create(options -> options.listenAddress(address).sslSelfSigned())));
  }
}
