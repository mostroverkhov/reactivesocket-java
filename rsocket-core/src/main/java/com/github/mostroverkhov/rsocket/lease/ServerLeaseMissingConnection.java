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

import com.github.mostroverkhov.rsocket.DuplexConnection;
import com.github.mostroverkhov.rsocket.DuplexConnectionProxy;
import com.github.mostroverkhov.rsocket.Frame;
import com.github.mostroverkhov.rsocket.FrameType;
import com.github.mostroverkhov.rsocket.exceptions.UnsupportedSetupException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

class ServerLeaseMissingConnection extends DuplexConnectionProxy {

  private final UnicastProcessor<Frame> setupFrames = UnicastProcessor.create();

  public ServerLeaseMissingConnection(DuplexConnection source) {
    super(source);
  }

  @Override
  public Flux<Frame> receive() {
    super.receive()
        .next()
        .subscribe(
            frame -> {
              if (frame.getType().equals(FrameType.SETUP) && Frame.Setup.supportsLease(frame)) {
                frame.release();
                UnsupportedSetupException error =
                    new UnsupportedSetupException("Server does not support Lease");
                sendOne(Frame.Error.from(0, error))
                    .then(close())
                    .onErrorResume(err -> Mono.empty())
                    .subscribe();
              } else {
                setupFrames.onNext(frame);
              }
            },
            setupFrames::onError);
    return setupFrames;
  }
}
