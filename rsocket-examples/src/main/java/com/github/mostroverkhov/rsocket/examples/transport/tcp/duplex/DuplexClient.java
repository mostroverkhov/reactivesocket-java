/*
 * Copyright 2017 Netflix, Inc.
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

package com.github.mostroverkhov.rsocket.examples.transport.tcp.duplex;

import com.github.mostroverkhov.rsocket.AbstractRSocket;
import com.github.mostroverkhov.rsocket.Payload;
import com.github.mostroverkhov.rsocket.RSocket;
import com.github.mostroverkhov.rsocket.RSocketFactory;
import com.github.mostroverkhov.rsocket.transport.netty.client.TcpClientTransport;
import com.github.mostroverkhov.rsocket.transport.netty.server.TcpServerTransport;
import com.github.mostroverkhov.rsocket.util.PayloadImpl;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class DuplexClient {

  public static void main(String[] args) {
    RSocketFactory.receive()
        .acceptor(
            (setup, reactiveSocket) -> {
              reactiveSocket
                  .requestStream(new PayloadImpl("Hello-Bidi"))
                  .map(Payload::getDataUtf8)
                  .log()
                  .subscribe();

              return Mono.just(new AbstractRSocket() {});
            })
        .transport(TcpServerTransport.create("localhost", 7000))
        .start()
        .subscribe();

    RSocket socket =
        RSocketFactory.connect()
            .acceptor(
                rSocket ->
                    new AbstractRSocket() {
                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        return Flux.interval(Duration.ofSeconds(1))
                            .map(aLong -> new PayloadImpl("Bi-di Response => " + aLong));
                      }
                    })
            .transport(TcpClientTransport.create("localhost", 7000))
            .start()
            .block();

    socket.onClose().block();
  }
}
