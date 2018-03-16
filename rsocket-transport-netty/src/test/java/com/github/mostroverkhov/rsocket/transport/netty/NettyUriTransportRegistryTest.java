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

import static org.junit.Assert.assertTrue;

import com.github.mostroverkhov.rsocket.transport.ClientTransport;
import com.github.mostroverkhov.rsocket.transport.ServerTransport;
import com.github.mostroverkhov.rsocket.transport.netty.client.TcpClientTransport;
import com.github.mostroverkhov.rsocket.transport.netty.client.WebsocketClientTransport;
import com.github.mostroverkhov.rsocket.transport.netty.server.TcpServerTransport;
import com.github.mostroverkhov.rsocket.transport.netty.server.WebsocketServerTransport;
import com.github.mostroverkhov.rsocket.uri.UriTransportRegistry;
import org.junit.Test;

public class NettyUriTransportRegistryTest {
  @Test
  public void testTcpClient() {
    ClientTransport transport = UriTransportRegistry.clientForUri("tcp://localhost:9898");

    assertTrue(transport instanceof TcpClientTransport);
  }

  @Test
  public void testTcpServer() {
    ServerTransport transport = UriTransportRegistry.serverForUri("tcp://localhost:9898");

    assertTrue(transport instanceof TcpServerTransport);
  }

  @Test
  public void testWsClient() {
    ClientTransport transport = UriTransportRegistry.clientForUri("ws://localhost:9898");

    assertTrue(transport instanceof WebsocketClientTransport);
  }

  @Test
  public void testWsServer() {
    ServerTransport transport = UriTransportRegistry.serverForUri("ws://localhost:9898");

    assertTrue(transport instanceof WebsocketServerTransport);
  }
}
