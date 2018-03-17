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

import com.github.mostroverkhov.rsocket.Payload;
import com.github.mostroverkhov.rsocket.RSocket;
import com.github.mostroverkhov.rsocket.util.RSocketProxy;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class LeaseRSocket extends RSocketProxy {
  private final LeaseContext leaseContext;
  private final String tag;
  private final LeaseManager leaseManager;

  public LeaseRSocket(
      LeaseContext leaseContext, RSocket source, String tag, LeaseManager leaseManager) {
    super(source);
    this.leaseContext = leaseContext;
    this.tag = tag;
    this.leaseManager = leaseManager;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return request(super.fireAndForget(payload));
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return request(super.requestResponse(payload));
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return request(super.requestStream(payload));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return request(super.requestChannel(payloads));
  }

  @Override
  public double availability() {
    return Math.min(super.availability(), leaseManager.availability());
  }

  private <K> Mono<K> request(Mono<K> actual) {
    return request(Mono::defer, actual, Mono::error);
  }

  private <K> Flux<K> request(Flux<K> actual) {
    return request(Flux::defer, actual, Flux::error);
  }

  private <K, T extends Publisher<K>> T request(
      Function<Supplier<T>, T> defer, T actual, Function<? super Throwable, ? extends T> error) {

    return defer.apply(
        () -> {
          if (isEnabled()) {
            try {
              leaseManager.useLease();
              return actual;
            } catch (Exception e) {
              return error.apply(e);
            }
          } else {
            return actual;
          }
        });
  }

  @Override
  public String toString() {
    return "LeaseRSocket{"
        + "leaseContext="
        + leaseContext
        + ", tag='"
        + tag
        + '\''
        + ", leaseManager="
        + leaseManager
        + '}';
  }

  private boolean isEnabled() {
    return leaseContext.isLeaseEnabled();
  }
}
