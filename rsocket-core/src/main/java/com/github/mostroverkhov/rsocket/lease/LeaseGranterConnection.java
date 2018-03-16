/*
 * Copyright 2018 Maksym Ostroverkhov
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

package com.github.mostroverkhov.rsocket.lease;

import com.github.mostroverkhov.rsocket.DuplexConnection;
import com.github.mostroverkhov.rsocket.DuplexConnectionProxy;
import com.github.mostroverkhov.rsocket.Frame;
import com.github.mostroverkhov.rsocket.FrameType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class LeaseGranterConnection extends DuplexConnectionProxy implements LeaseGranter {
  private final LeaseContext leaseContext;
  private final LeaseManager sendManager;
  private final LeaseManager receiveManager;

  public LeaseGranterConnection(
      LeaseContext leaseContext,
      DuplexConnection source,
      LeaseManager sendManager,
      LeaseManager receiveManager) {
    super(source);
    this.leaseContext = leaseContext;
    this.sendManager = sendManager;
    this.receiveManager = receiveManager;
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
