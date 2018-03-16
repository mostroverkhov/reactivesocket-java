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

import com.github.mostroverkhov.rsocket.RSocketFactory;
import com.github.mostroverkhov.rsocket.aeron.internal.AeronWrapper;
import com.github.mostroverkhov.rsocket.aeron.internal.DefaultAeronWrapper;
import com.github.mostroverkhov.rsocket.aeron.internal.EventLoop;
import com.github.mostroverkhov.rsocket.aeron.internal.SingleThreadedEventLoop;
import com.github.mostroverkhov.rsocket.aeron.internal.reactivestreams.AeronSocketAddress;
import com.github.mostroverkhov.rsocket.aeron.server.AeronServerTransport;
import com.github.mostroverkhov.rsocket.test.PingHandler;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

public final class AeronPongServer {
  static {
    final io.aeron.driver.MediaDriver.Context ctx =
        new io.aeron.driver.MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED_NETWORK)
            .dirDeleteOnStart(true);
    MediaDriver.launch(ctx);
  }

  public static void main(String... args) {
    MediaDriverHolder.getInstance();
    AeronWrapper aeronWrapper = new DefaultAeronWrapper();

    AeronSocketAddress serverManagementSocketAddress =
        AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
    EventLoop serverEventLoop = new SingleThreadedEventLoop("server");
    AeronServerTransport server =
        new AeronServerTransport(aeronWrapper, serverManagementSocketAddress, serverEventLoop);

    AeronServerTransport transport =
        new AeronServerTransport(aeronWrapper, serverManagementSocketAddress, serverEventLoop);
    RSocketFactory.receive().acceptor(new PingHandler()).transport(transport).start();
  }
}
