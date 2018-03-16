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

import com.github.mostroverkhov.rsocket.DuplexConnection;
import com.github.mostroverkhov.rsocket.interceptors.DuplexConnectionInterceptor;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class KeepAliveRequesterInterceptor implements DuplexConnectionInterceptor {
  private final Duration period;
  private final int ticksTimeout;
  private final Supplier<ByteBuffer> payloadSupplier;
  private final Consumer<Throwable> errorConsumer;
  private final Consumer<KeepAlives> keepAlivesConsumer;

  public KeepAliveRequesterInterceptor(
      Duration period,
      int ticksTimeout,
      Supplier<ByteBuffer> payloadSupplier,
      Consumer<Throwable> errorConsumer,
      Consumer<KeepAlives> keepAlivesConsumer) {
    this.period = period;
    this.ticksTimeout = ticksTimeout;
    this.payloadSupplier = payloadSupplier;
    this.errorConsumer = errorConsumer;
    this.keepAlivesConsumer = keepAlivesConsumer;
  }

  @Override
  public DuplexConnection apply(Type type, DuplexConnection connection) {
    if (type == Type.STREAM_ZERO) {
      KeepAliveRequesterConnection keepAliveRequesterConnection =
          new KeepAliveRequesterConnection(
              connection, period, ticksTimeout, payloadSupplier, errorConsumer);
      keepAlivesConsumer.accept(
          new KeepAlives(
              keepAliveRequesterConnection.keepAliveAvailable(),
              keepAliveRequesterConnection.keepAliveMissing(),
              keepAliveRequesterConnection.close()));

      return keepAliveRequesterConnection;
    } else {
      return connection;
    }
  }
}
