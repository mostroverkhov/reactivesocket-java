/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package com.github.mostroverkhov.rsocket.examples.transport.tcp.stream;

import com.github.mostroverkhov.rsocket.*;
import com.github.mostroverkhov.rsocket.transport.netty.client.TcpClientTransport;
import com.github.mostroverkhov.rsocket.transport.netty.server.TcpServerTransport;
import com.github.mostroverkhov.rsocket.util.PayloadImpl;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class StreamingClient {

  public static void main(String[] args) {
    RSocketFactory.receive()
        .acceptor(new SocketAcceptorImpl())
        .transport(TcpServerTransport.create("localhost", 7000))
        .start()
        .subscribe();

    RSocket socket =
        RSocketFactory.connect()
            .transport(TcpClientTransport.create("localhost", 7000))
            .start()
            .block();

    socket
        .requestStream(new PayloadImpl("Hello"))
        .map(Payload::getDataUtf8)
        .doOnNext(System.out::println)
        .take(10)
        .thenEmpty(socket.close())
        .block();
  }

  private static class SocketAcceptorImpl implements SocketAcceptor {
    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setupPayload, RSocket reactiveSocket) {
      return Mono.just(
          new AbstractRSocket() {
            @Override
            public Flux<Payload> requestStream(Payload payload) {
              return Flux.interval(Duration.ofMillis(100))
                  .map(aLong -> new PayloadImpl("Interval: " + aLong));
            }
          });
    }
  }
}
