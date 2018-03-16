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

import com.github.mostroverkhov.rsocket.exceptions.ConnectionException;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;

public class CloseOnKeepAliveTimeout implements Consumer<KeepAlives> {
  private final Consumer<Throwable> errConsumer;

  public CloseOnKeepAliveTimeout(Consumer<Throwable> errConsumer) {
    this.errConsumer = errConsumer;
  }

  @Override
  public void accept(KeepAlives keepAlives) {
    keepAlives
        .keepAliveMissing()
        .next()
        .flatMap(keepAliveMissing -> keepAlives.closeConnection().then(Mono.just(keepAliveMissing)))
        .subscribe(
            keepAliveMissing -> errConsumer.accept(keepAliveMissingError(keepAliveMissing)),
            errConsumer);
  }

  private Exception keepAliveMissingError(KeepAliveMissing keepAliveMissing) {
    String message =
        String.format(
            "Missed %d keep-alive acks with ack timeout of %d ms",
            keepAliveMissing.timeoutTicks(), keepAliveMissing.tickPeriod().toMillis());
    return new ConnectionException(message);
  }
}
