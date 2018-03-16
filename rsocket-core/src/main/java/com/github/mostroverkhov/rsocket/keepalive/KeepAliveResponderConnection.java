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
import com.github.mostroverkhov.rsocket.DuplexConnectionProxy;
import com.github.mostroverkhov.rsocket.Frame;
import com.github.mostroverkhov.rsocket.FrameType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.UnicastProcessor;

public class KeepAliveResponderConnection extends DuplexConnectionProxy {

  private final FluxProcessor<Frame, Frame> sender = UnicastProcessor.create();

  public KeepAliveResponderConnection(DuplexConnection zeroConn) {
    super(zeroConn);
    zeroConn.send(sender).subscribe(Void -> {}, err -> {});
  }

  @Override
  public Flux<Frame> receive() {
    return super.receive()
        .doOnNext(
            f -> {
              if (isKeepAliveRequest(f)) {
                ByteBuf data = Unpooled.wrappedBuffer(f.getData());
                sender.onNext(Frame.Keepalive.from(data, false));
              }
            });
  }

  private boolean isKeepAliveRequest(Frame f) {
    return f.getType().equals(FrameType.KEEPALIVE) && Frame.Keepalive.hasRespondFlag(f);
  }
}
