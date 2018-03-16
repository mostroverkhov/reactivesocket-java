package com.github.mostroverkhov.rsocket.integration;

import static org.junit.Assert.assertEquals;

import com.github.mostroverkhov.rsocket.*;
import com.github.mostroverkhov.rsocket.exceptions.UnsupportedSetupException;
import com.github.mostroverkhov.rsocket.transport.local.LocalClientTransport;
import com.github.mostroverkhov.rsocket.transport.local.LocalServerTransport;
import com.github.mostroverkhov.rsocket.util.PayloadImpl;
import java.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.test.StepVerifier;

public class ServerLeaseDisabledTest {

  private static final String WITHOUT_LEASE = "withoutLease";

  private Closeable start;
  private LocalServerTransport serverTransport;

  @Before
  public void setUp() throws Exception {
    serverTransport = LocalServerTransport.create(WITHOUT_LEASE);
    start =
        RSocketFactory.receive()
            .disableLease()
            .acceptor(
                (setup, sendingSocket) ->
                    Mono.just(
                        new AbstractRSocket() {
                          @Override
                          public Mono<Payload> requestResponse(Payload payload) {
                            return Mono.just(payload);
                          }
                        }))
            .transport(serverTransport)
            .start()
            .block();
  }

  @After
  public void tearDown() throws Exception {
    start.close().block();
  }

  @Test
  public void clientNoLease() throws Exception {
    RSocket requester =
        RSocketFactory.connect()
            .disableLease()
            .transport(LocalClientTransport.create(WITHOUT_LEASE))
            .start()
            .block();
    assertEquals(1.0f, requester.availability(), 1e-5);

    Mono<Payload> request = requester.requestResponse(new PayloadImpl("data"));
    StepVerifier.create(request)
        .expectNextMatches(p -> "data".equals(p.getDataUtf8()))
        .expectComplete()
        .verify();
  }

  @Test
  public void clientLease() throws Exception {
    MonoProcessor<Throwable> error = MonoProcessor.create();

    RSocket requester =
        RSocketFactory.connect()
            .errorConsumer(error::onNext)
            .enableLease(connRef -> {})
            .transport(LocalClientTransport.create(WITHOUT_LEASE))
            .start()
            .block();

    StepVerifier.create(error)
        .expectNextMatches(
            err ->
                err instanceof UnsupportedSetupException
                    && "Server does not support Lease".equals(err.getMessage()))
        .expectComplete()
        .verify(Duration.ofSeconds(2));
  }
}
