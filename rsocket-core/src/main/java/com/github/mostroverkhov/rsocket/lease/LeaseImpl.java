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

package com.github.mostroverkhov.rsocket.lease;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

class LeaseImpl implements Lease {
  private final int ttl;
  private final AtomicInteger numberOfRequests;
  private final int startingNumberOfRequests;
  private final ByteBuffer metadata;
  private final long expiry;

  public LeaseImpl(int numberOfRequests, int ttl, @Nullable ByteBuffer metadata) {
    assertNumberOfRequests(numberOfRequests, ttl);
    this.numberOfRequests = new AtomicInteger(numberOfRequests);
    this.startingNumberOfRequests = numberOfRequests;
    this.ttl = ttl;
    this.metadata = metadata;
    this.expiry = now() + ttl * 1000;
  }

  static LeaseImpl invalidLease() {
    return new LeaseImpl(0, 0, null);
  }

  public int getTtl() {
    return ttl;
  }

  @Override
  public int getAllowedRequests() {
    return Math.max(0, numberOfRequests.get());
  }

  public ByteBuffer getMetadata() {
    return metadata;
  }

  @Override
  public long expiry() {
    return expiry;
  }

  public double availability() {
    return isValid() ? getAllowedRequests() / (double) startingNumberOfRequests : 0.0;
  }

  @Override
  public boolean isValid() {
    return startingNumberOfRequests > 0 && getAllowedRequests() > 0 && !isExpired();
  }

  public boolean use(int useRequestCount) {
    assertUseRequests(useRequestCount);
    int available =
        numberOfRequests.accumulateAndGet(
            useRequestCount, (cur, update) -> Math.max(-1, cur - update));
    return available >= 0 && !isExpired();
  }

  static void assertUseRequests(int useRequestCount) {
    if (useRequestCount <= 0) {
      throw new IllegalArgumentException("Number of requests must be positive");
    }
  }

  private long now() {
    return System.currentTimeMillis();
  }

  private static void assertNumberOfRequests(int numberOfRequests, int ttl) {
    if (numberOfRequests < 0) {
      throw new IllegalArgumentException("Number of requests must be non-negative");
    }
    if (ttl < 0) {
      throw new IllegalArgumentException("Time-to-live must be non-negative");
    }
  }
}
