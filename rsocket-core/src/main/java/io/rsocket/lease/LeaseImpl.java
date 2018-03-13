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

package io.rsocket.lease;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

class LeaseImpl implements Lease {
  private final int ttl;
  protected final AtomicInteger numberOfRequests;
  private final int startingNumberOfRequests;
  private final ByteBuffer metadata;
  private final long expiry;

  public LeaseImpl(int numberOfRequests, int ttl, @Nullable ByteBuffer metadata) {
    assertNumberOfRequests(numberOfRequests);
    this.numberOfRequests = new AtomicInteger(numberOfRequests);
    this.startingNumberOfRequests = numberOfRequests;
    this.ttl = ttl;
    this.metadata = metadata;
    this.expiry = now() + ttl * 1000;
  }

  public int getTtl() {
    return ttl;
  }

  @Override
  public int getAllowedRequests() {
    return numberOfRequests.get();
  }

  public ByteBuffer getMetadata() {
    return metadata;
  }

  @Override
  public long expiry() {
    return expiry;
  }

  public boolean isValid() {
    return now() <= expiry() && getAllowedRequests() > 0;
  }

  public double availability() {
    return isValid() ? getAllowedRequests() / (double) startingNumberOfRequests : 0.0;
  }

  public void use(int useRequestCount) {
    assertUseRequests(useRequestCount);
    numberOfRequests.accumulateAndGet(useRequestCount, (cur, update) -> Math.max(0, cur - update));
  }

  static void assertUseRequests(int useRequestCount) {
    if (useRequestCount <= 0) {
      throw new IllegalArgumentException("Number of requests should be positive");
    }
  }

  private long now() {
    return System.currentTimeMillis();
  }

  private static void assertNumberOfRequests(int numberOfRequest) {
    if (numberOfRequest < 0) {
      throw new IllegalArgumentException("Number of requests should be positive");
    }
  }
}
