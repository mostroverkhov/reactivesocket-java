package io.rsocket.lease;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.DuplexConnection;
import io.rsocket.DuplexConnectionProxy;
import io.rsocket.Frame;
import io.rsocket.FrameType;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class LeaseConnection extends DuplexConnectionProxy implements LeaseGranter {
  private static final LeaseContext alwaysEnabled = new LeaseContext();
  private final LeaseContext leaseContext;
  private final LeaseManager sendManager;
  private final LeaseManager receiveManager;

  public LeaseConnection(
      LeaseContext leaseContext,
      DuplexConnection source,
      LeaseManager sendManager,
      LeaseManager receiveManager) {
    super(source);
    this.leaseContext = leaseContext;
    this.sendManager = sendManager;
    this.receiveManager = receiveManager;
  }

  public LeaseConnection(
      DuplexConnection source, LeaseManager sendManager, LeaseManager receiveManager) {
    this(alwaysEnabled, source, sendManager, receiveManager);
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frame) {
    return super.send(Flux.from(frame).doOnNext(f -> leaseGrantedTo(f, receiveManager)));
  }

  @Override
  public Flux<Frame> receive() {
    return super.receive().doOnNext(f -> leaseGrantedTo(f, sendManager));
  }

  private void leaseGrantedTo(Frame f, LeaseManager leaseManager) {
    if (isEnabled() && isLease(f)) {
      int requests = Frame.Lease.numberOfRequests(f);
      int ttl = Frame.Lease.ttl(f);
      leaseManager.grantLease(requests, ttl);
    }
  }

  private boolean isEnabled() {
    return leaseContext.isLeaseEnabled();
  }

  @Override
  public Mono<Void> grantLease(int requests, int ttl, ByteBuffer metadata) {
    ByteBuf byteBuf = metadata == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(metadata);

    if (ttl <= 0) {
      return Mono.error(new IllegalArgumentException("Ttl should be positive"));
    }
    if (requests <= 0) {
      return Mono.error(new IllegalArgumentException("Allowed requests should be positive"));
    }
    return sendOne(Frame.Lease.from(ttl, requests, byteBuf));
  }

  private boolean isLease(Frame f) {
    return f.getType() == FrameType.LEASE;
  }
}
