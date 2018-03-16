package com.github.mostroverkhov.rsocket.lease;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.github.mostroverkhov.rsocket.Frame;
import com.github.mostroverkhov.rsocket.FrameType;
import com.github.mostroverkhov.rsocket.test.util.LocalDuplexConnection;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Mono;

public class LeaseGranterConnectionTest {

  private LeaseGranterConnection leaseGranterConnection;
  private DirectProcessor<Frame> sender;
  private DirectProcessor<Frame> receiver;
  private LeaseManager send;
  private LeaseManager receive;

  @Before
  public void setUp() throws Exception {
    LeaseContext leaseContext = new LeaseContext();
    send = new LeaseManager("send");
    receive = new LeaseManager("receive");
    sender = DirectProcessor.create();
    receiver = DirectProcessor.create();
    LocalDuplexConnection local = new LocalDuplexConnection("test", sender, receiver);
    leaseGranterConnection = new LeaseGranterConnection(leaseContext, local, send, receive);
  }

  @Test
  public void sentLease() throws Exception {
    leaseGranterConnection.send(Mono.just(Frame.Lease.from(2, 1, Unpooled.EMPTY_BUFFER))).block();
    assertEquals(1.0, receive.availability(), 1e-5);
    assertEquals(0.0, send.availability(), 1e-5);
  }

  @Test
  public void receivedLease() throws Exception {
    leaseGranterConnection.receive().subscribe();
    receiver.onNext(Frame.Lease.from(2, 1, Unpooled.EMPTY_BUFFER));
    assertEquals(0.0, receive.availability(), 1e-5);
    assertEquals(1.0, send.availability(), 1e-5);
  }

  @Test
  public void grantLease() throws Exception {
    Mono.defer(() -> leaseGranterConnection.grantLease(2, 1, ByteBuffer.allocateDirect(0)))
        .delaySubscription(Duration.ofMillis(100))
        .subscribe();
    Frame f = sender.next().block();

    assertNotNull(f);
    assertTrue(f.getType() == FrameType.LEASE);
    assertEquals(2, Frame.Lease.numberOfRequests(f));
    assertEquals(1, Frame.Lease.ttl(f));
  }
}
