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
package com.github.mostroverkhov.rsocket.aeron;

import com.github.mostroverkhov.rsocket.DuplexConnection;
import com.github.mostroverkhov.rsocket.Frame;
import com.github.mostroverkhov.rsocket.aeron.internal.reactivestreams.AeronChannel;
import io.netty.buffer.Unpooled;
import org.agrona.concurrent.UnsafeBuffer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/** Implementation of {@link DuplexConnection} over Aeron using an {@link AeronChannel} */
public class AeronDuplexConnection implements DuplexConnection {
  private final String name;
  private final AeronChannel channel;
  private final MonoProcessor<Void> emptySubject;

  public AeronDuplexConnection(String name, AeronChannel channel) {
    this.name = name;
    this.channel = channel;
    this.emptySubject = MonoProcessor.create();
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frame) {
    Flux<UnsafeBuffer> buffers =
        Flux.from(frame).map(f -> new UnsafeBuffer(f.content().nioBuffer()));

    return channel.send(buffers);
  }

  @Override
  public Flux<Frame> receive() {
    return channel
        .receive()
        .map(b -> Frame.from(Unpooled.wrappedBuffer(b.byteBuffer())))
        .doOnError(Throwable::printStackTrace);
  }

  @Override
  public double availability() {
    return channel.isActive() ? 1.0 : 0.0;
  }

  @Override
  public Mono<Void> close() {
    return Mono.defer(
        () -> {
          try {
            channel.close();
            emptySubject.onComplete();
          } catch (Exception e) {
            emptySubject.onError(e);
          }
          return emptySubject;
        });
  }

  @Override
  public Mono<Void> onClose() {
    return emptySubject;
  }

  @Override
  public String toString() {
    return "AeronDuplexConnection{"
        + "name='"
        + name
        + '\''
        + ", channel="
        + channel
        + ", emptySubject="
        + emptySubject
        + '}';
  }
}
