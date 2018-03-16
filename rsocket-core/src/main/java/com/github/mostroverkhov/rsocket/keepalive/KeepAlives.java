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

package com.github.mostroverkhov.rsocket.keepalive;

import java.nio.ByteBuffer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class KeepAlives {
  private final Flux<ByteBuffer> keepAliveAvailable;
  private final Flux<KeepAliveMissing> keepAliveMissing;
  private final Mono<Void> closeConnection;

  public KeepAlives(
      Flux<ByteBuffer> keepAliveAvailable,
      Flux<KeepAliveMissing> keepAliveMissing,
      Mono<Void> closeConnection) {
    this.keepAliveAvailable = keepAliveAvailable;
    this.keepAliveMissing = keepAliveMissing;
    this.closeConnection = closeConnection;
  }

  public Flux<ByteBuffer> keepAlive() {
    return keepAliveAvailable;
  }

  public Flux<KeepAliveMissing> keepAliveMissing() {
    return keepAliveMissing;
  }

  public Mono<Void> closeConnection() {
    return closeConnection;
  }
}
