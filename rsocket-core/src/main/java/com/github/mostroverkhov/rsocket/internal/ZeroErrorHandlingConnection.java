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

import com.github.mostroverkhov.rsocket.DuplexConnection;
import com.github.mostroverkhov.rsocket.DuplexConnectionProxy;
import com.github.mostroverkhov.rsocket.Frame;
import com.github.mostroverkhov.rsocket.FrameType;
import com.github.mostroverkhov.rsocket.exceptions.Exceptions;
import reactor.core.publisher.Flux;

public class ZeroErrorHandlingConnection extends DuplexConnectionProxy {

  public ZeroErrorHandlingConnection(DuplexConnection source) {
    super(source);
  }

  @Override
  public Flux<Frame> receive() {
    return super.receive()
        .doOnNext(
            f -> {
              if (isZeroStreamError(f)) {
                RuntimeException err = Exceptions.from(f);
                f.release();
                throw err;
              }
            });
  }

  private boolean isZeroStreamError(Frame f) {
    return f.getStreamId() == 0 && f.getType() == FrameType.ERROR;
  }
}
