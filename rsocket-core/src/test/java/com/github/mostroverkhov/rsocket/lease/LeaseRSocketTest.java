package com.github.mostroverkhov.rsocket.lease;

import static org.junit.Assert.assertEquals;

import com.github.mostroverkhov.rsocket.Payload;
import com.github.mostroverkhov.rsocket.RSocket;
import com.github.mostroverkhov.rsocket.exceptions.NoLeaseException;
import com.github.mostroverkhov.rsocket.util.PayloadImpl;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class LeaseRSocketTest {

  private LeaseRSocket leaseRSocket;
  private MockRSocket rSocket;
  private LeaseManager leaseManager;

  @Before
  public void setUp() throws Exception {
    rSocket = new MockRSocket();
    leaseManager = new LeaseManager("");
    LeaseContext leaseEnabled = new LeaseContext();
    leaseRSocket = new LeaseRSocket(leaseEnabled, rSocket, "", leaseManager);
  }

  @Test
  public void grantedLease() throws Exception {
    leaseManager.grantLease(2, 1);
    assertEquals(1.0, leaseRSocket.availability(), 1e-5);
  }

  @Test
  public void usedLease() throws Exception {
    leaseManager.grantLease(2, 1);
    leaseRSocket.fireAndForget(new PayloadImpl("test")).subscribe();
    assertEquals(0.5, leaseRSocket.availability(), 1e-5);
  }

  @Test
  public void depletedLease() throws Exception {
    leaseManager.grantLease(1, 1);
    leaseRSocket.fireAndForget(new PayloadImpl("test")).subscribe();
    StepVerifier.create(leaseRSocket.fireAndForget(new PayloadImpl("test")))
        .expectError(NoLeaseException.class)
        .verify();
  }

  @Test
  public void connectionNotAvailable() throws Exception {
    leaseManager.grantLease(1, 1);
    rSocket.setAvailability(0.0f);
    assertEquals(0.0, leaseRSocket.availability(), 1e-5);
  }

  private static class MockRSocket implements RSocket {
    private float availability = 1.0f;

    public void setAvailability(float availability) {
      this.availability = availability;
    }

    @Override
    public double availability() {
      return availability;
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      return Mono.empty();
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return Mono.just(payload);
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      return Flux.just(payload);
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return Flux.from(payloads);
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
      return Mono.empty();
    }

    @Override
    public Mono<Void> close() {
      return Mono.empty();
    }

    @Override
    public Mono<Void> onClose() {
      return Mono.empty();
    }
  }
}
