package io.rsocket.integration;

import static org.junit.Assert.assertEquals;

import io.rsocket.*;
import io.rsocket.exceptions.NoLeaseException;
import io.rsocket.lease.LeaseConnectionRef;
import io.rsocket.transport.local.LocalClientTransport;
import io.rsocket.transport.local.LocalServerTransport;
import io.rsocket.util.PayloadImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.test.StepVerifier;

public class ServerLeaseEnabledTest {

  private static final String WITH_LEASE = "withLease";

  private Closeable start;
  private LocalServerTransport serverTransport;
  private MonoProcessor<LeaseConnectionRef> serverLeaseGranter = MonoProcessor.create();

  @Before
  public void setUp() throws Exception {
    serverTransport = LocalServerTransport.create(WITH_LEASE);
    start =
        RSocketFactory.receive()
            .enableLease(ref -> serverLeaseGranter.onNext(ref))
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
            .transport(LocalClientTransport.create(WITH_LEASE))
            .start()
            .block();
    assertEquals(1.0f, requester.availability(), 1e-5);

    Mono<Payload> request = requester.requestResponse(new PayloadImpl("data"));
    StepVerifier.create(request).expectNextMatches(this::isData).expectComplete().verify();
  }

  @Test
  public void clientLeaseRequestWithoutLease() throws Exception {

    RSocket requester =
        RSocketFactory.connect()
            .enableLease(ref -> {})
            .transport(LocalClientTransport.create(WITH_LEASE))
            .start()
            .block();

    assertEquals(0.0, requester.availability(), 1e-5);

    StepVerifier.create(requester.requestResponse(new PayloadImpl("data")))
        .expectError(NoLeaseException.class)
        .verify();
  }

  @Test
  public void clientLeaseRequestWithLease() throws Exception {

    RSocket requester =
        RSocketFactory.connect()
            .enableLease(ref -> {})
            .transport(LocalClientTransport.create(WITH_LEASE))
            .start()
            .block();

    Mono<Payload> request = requester.requestResponse(getData());
    Flux<Payload> requests =
        serverLeaseGranter
            .flatMap(ref -> ref.grantLease(2, 2))
            .doOnTerminate(() -> assertEquals(1.0, requester.availability(), 1e-5))
            .then(request)
            .doOnTerminate(() -> assertEquals(0.5, requester.availability(), 1e-5))
            .concatWith(request)
            .doOnTerminate(() -> assertEquals(0.0, requester.availability(), 1e-5))
            .concatWith(request);

    StepVerifier.create(requests)
        .expectNextMatches(this::isData)
        .expectNextMatches(this::isData)
        .expectError(NoLeaseException.class)
        .verify();
  }

  private PayloadImpl getData() {
    return new PayloadImpl("data");
  }

  private boolean isData(Payload p) {
    return p.getDataUtf8().equals("data");
  }
}
