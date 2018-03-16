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

import java.nio.ByteBuffer;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import reactor.core.publisher.Mono;

/** Provides means to grant lease to peer */
public class LeaseConnectionRef {
  private final LeaseGranter leaseGranter;
  private final Mono<Void> onClose;

  LeaseConnectionRef(LeaseGranter granter, @Nonnull Mono<Void> onClose) {
    Objects.requireNonNull(granter, "leaseGranter");
    Objects.requireNonNull(onClose, "onClose");
    this.onClose = onClose;
    this.leaseGranter = granter;
  }

  public Mono<Void> grantLease(
      int numberOfRequests, long ttlSeconds, @Nullable ByteBuffer metadata) {
    assertArgs(numberOfRequests, ttlSeconds);
    int ttl = Math.toIntExact(ttlSeconds);
    return leaseGranter.grantLease(numberOfRequests, ttl, metadata);
  }

  public Mono<Void> grantLease(int numberOfRequests, long timeToLive) {
    return grantLease(numberOfRequests, timeToLive, ByteBuffer.allocate(0));
  }

  public Mono<Void> onClose() {
    return onClose;
  }

  private void assertArgs(int numberOfRequests, long ttl) {
    if (numberOfRequests <= 0) {
      throw new IllegalArgumentException("numberOfRequests must be non-negative");
    }

    if (ttl <= 0) {
      throw new IllegalArgumentException("timeToLive must be non-negative");
    }
  }
}
