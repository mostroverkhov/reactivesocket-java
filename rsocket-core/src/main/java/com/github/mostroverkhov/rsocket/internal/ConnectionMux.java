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

package com.github.mostroverkhov.rsocket.internal;

import static java.util.stream.Collectors.toList;

import com.github.mostroverkhov.rsocket.DuplexConnection;
import com.github.mostroverkhov.rsocket.Frame;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ConnectionMux implements DuplexConnection {
  private final DuplexConnection sender;
  private final List<DuplexConnection> demuxed;

  public ConnectionMux(DuplexConnection sender, DuplexConnection... demuxed) {
    this.sender = sender;
    this.demuxed = Arrays.asList(demuxed);
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frame) {
    return sender.send(frame);
  }

  @Override
  public Flux<Frame> receive() {
    return Flux.merge(transform(demuxed, DuplexConnection::receive));
  }

  @Override
  public double availability() {
    return sender.availability();
  }

  @Override
  public Mono<Void> close() {
    return whenCompleted(DuplexConnection::close);
  }

  @Override
  public Mono<Void> onClose() {
    return whenCompleted(DuplexConnection::onClose);
  }

  Mono<Void> whenCompleted(Function<DuplexConnection, Mono<Void>> mapper) {
    return Flux.fromIterable(demuxed).flatMap(mapper).then();
  }

  static <T> List<T> transform(List<DuplexConnection> col, Function<DuplexConnection, T> mapper) {
    return col.stream().map(mapper).collect(toList());
  }
}
