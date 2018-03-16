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

package com.github.mostroverkhov.rsocket.aeron.server;

import com.github.mostroverkhov.rsocket.Closeable;
import com.github.mostroverkhov.rsocket.DuplexConnection;
import com.github.mostroverkhov.rsocket.aeron.AeronDuplexConnection;
import com.github.mostroverkhov.rsocket.aeron.internal.AeronWrapper;
import com.github.mostroverkhov.rsocket.aeron.internal.EventLoop;
import com.github.mostroverkhov.rsocket.aeron.internal.reactivestreams.AeronChannelServer;
import com.github.mostroverkhov.rsocket.aeron.internal.reactivestreams.AeronSocketAddress;
import com.github.mostroverkhov.rsocket.transport.ServerTransport;
import reactor.core.publisher.Mono;

/** */
public class AeronServerTransport implements ServerTransport<Closeable> {
  private final AeronWrapper aeronWrapper;
  private final AeronSocketAddress managementSubscriptionSocket;
  private final EventLoop eventLoop;

  private AeronChannelServer aeronChannelServer;

  public AeronServerTransport(
      AeronWrapper aeronWrapper,
      AeronSocketAddress managementSubscriptionSocket,
      EventLoop eventLoop) {
    this.aeronWrapper = aeronWrapper;
    this.managementSubscriptionSocket = managementSubscriptionSocket;
    this.eventLoop = eventLoop;
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor) {
    synchronized (this) {
      if (aeronChannelServer != null) {
        throw new IllegalStateException("server already ready started");
      }

      aeronChannelServer =
          AeronChannelServer.create(
              aeronChannel -> {
                DuplexConnection connection = new AeronDuplexConnection("server", aeronChannel);
                acceptor.apply(connection).subscribe();
              },
              aeronWrapper,
              managementSubscriptionSocket,
              eventLoop);
    }

    return Mono.just(aeronChannelServer.start());
  }
}
