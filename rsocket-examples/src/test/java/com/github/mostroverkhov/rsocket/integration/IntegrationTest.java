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

package com.github.mostroverkhov.rsocket.integration;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.github.mostroverkhov.rsocket.AbstractRSocket;
import com.github.mostroverkhov.rsocket.Payload;
import com.github.mostroverkhov.rsocket.RSocket;
import com.github.mostroverkhov.rsocket.RSocketFactory;
import com.github.mostroverkhov.rsocket.interceptors.DuplexConnectionInterceptor;
import com.github.mostroverkhov.rsocket.interceptors.RSocketInterceptor;
import com.github.mostroverkhov.rsocket.test.TestSubscriber;
import com.github.mostroverkhov.rsocket.transport.netty.client.TcpClientTransport;
import com.github.mostroverkhov.rsocket.transport.netty.server.NettyContextCloseable;
import com.github.mostroverkhov.rsocket.transport.netty.server.TcpServerTransport;
import com.github.mostroverkhov.rsocket.util.PayloadImpl;
import com.github.mostroverkhov.rsocket.util.RSocketProxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class IntegrationTest {

  private NettyContextCloseable server;
  private RSocket client;
  private AtomicInteger requestCount;
  private CountDownLatch disconnectionCounter;
  public static volatile boolean calledClient = false;
  public static volatile boolean calledServer = false;
  public static volatile boolean calledFrame = false;

  private static final RSocketInterceptor clientPlugin;
  private static final RSocketInterceptor serverPlugin;
  private static final DuplexConnectionInterceptor connectionPlugin;

  static {
    clientPlugin =
        reactiveSocket ->
            new RSocketProxy(reactiveSocket) {
              @Override
              public Mono<Payload> requestResponse(Payload payload) {
                calledClient = true;
                return reactiveSocket.requestResponse(payload);
              }
            };

    serverPlugin =
        reactiveSocket ->
            new RSocketProxy(reactiveSocket) {
              @Override
              public Mono<Payload> requestResponse(Payload payload) {
                calledServer = true;
                return reactiveSocket.requestResponse(payload);
              }
            };

    connectionPlugin =
        (type, connection) -> {
          calledFrame = true;
          return connection;
        };
  }

  @Before
  public void startup() {
    requestCount = new AtomicInteger();
    disconnectionCounter = new CountDownLatch(1);

    TcpServerTransport serverTransport = TcpServerTransport.create(0);

    server =
        RSocketFactory.receive()
            .addHandlerInterceptor(serverPlugin)
            .addConnectionInterceptor(connectionPlugin)
            .acceptor(
                (setup, sendingSocket) -> {
                  sendingSocket
                      .onClose()
                      .doFinally(signalType -> disconnectionCounter.countDown())
                      .subscribe();

                  return Mono.just(
                      new AbstractRSocket() {
                        @Override
                        public Mono<Payload> requestResponse(Payload payload) {
                          return Mono.<Payload>just(new PayloadImpl("RESPONSE", "METADATA"))
                              .doOnSubscribe(s -> requestCount.incrementAndGet());
                        }

                        @Override
                        public Flux<Payload> requestStream(Payload payload) {
                          return Flux.range(1, 10_000).map(i -> new PayloadImpl("data -> " + i));
                        }
                      });
                })
            .transport(serverTransport)
            .start()
            .block();

    client =
        RSocketFactory.connect()
            .addRequesterInterceptor(clientPlugin)
            .addConnectionInterceptor(connectionPlugin)
            .transport(TcpClientTransport.create(server.address()))
            .start()
            .block();
  }

  @After
  public void teardown() {
    server.close().block();
  }

  @Test(timeout = 5_000L)
  public void testRequest() {
    client.requestResponse(new PayloadImpl("REQUEST", "META")).block();
    assertThat("Server did not see the request.", requestCount.get(), is(1));
    assertTrue(calledClient);
    assertTrue(calledServer);
    assertTrue(calledFrame);
  }

  @Test
  public void testStream() {
    Subscriber<Payload> subscriber = TestSubscriber.createCancelling();
    client.requestStream(new PayloadImpl("start")).subscribe(subscriber);

    verify(subscriber).onSubscribe(any());
    verifyNoMoreInteractions(subscriber);
  }

  @Test(timeout = 5_000L)
  public void testClose() throws InterruptedException {
    client.close().block();
    disconnectionCounter.await();
  }
}
