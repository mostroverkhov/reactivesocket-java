/*
 * Copyright 2016 Netflix, Inc.
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

import static org.junit.Assert.assertEquals;

import com.github.mostroverkhov.rsocket.Frame;
import com.github.mostroverkhov.rsocket.frame.SetupFrameFlyweight;
import com.github.mostroverkhov.rsocket.interceptors.InterceptorRegistry;
import com.github.mostroverkhov.rsocket.test.util.TestDuplexConnection;
import com.github.mostroverkhov.rsocket.util.PayloadImpl;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

public class ClientServerInputMultiplexerTest {
  private TestDuplexConnection source;
  private ConnectionDemux multiplexer;

  @Before
  public void setup() {
    source = new TestDuplexConnection();
    multiplexer = new ConnectionDemux(source, new InterceptorRegistry());
  }

  @Test
  public void testSplits() {
    AtomicInteger clientFrames = new AtomicInteger();
    AtomicInteger serverFrames = new AtomicInteger();
    AtomicInteger connectionFrames = new AtomicInteger();
    AtomicInteger initFrames = new AtomicInteger();

    multiplexer
        .asClientConnection()
        .receive()
        .doOnNext(f -> clientFrames.incrementAndGet())
        .subscribe();
    multiplexer
        .asServerConnection()
        .receive()
        .doOnNext(f -> serverFrames.incrementAndGet())
        .subscribe();
    multiplexer
        .asStreamZeroConnection()
        .receive()
        .doOnNext(f -> connectionFrames.incrementAndGet())
        .subscribe();
    multiplexer
        .asInitConnection()
        .receive()
        .doOnNext(f -> initFrames.incrementAndGet())
        .subscribe();

    source.addToReceivedBuffer(Frame.Error.from(1, new Exception()));
    assertEquals(1, clientFrames.get());
    assertEquals(0, serverFrames.get());
    assertEquals(0, connectionFrames.get());
    assertEquals(0, initFrames.get());

    source.addToReceivedBuffer(Frame.Error.from(2, new Exception()));
    assertEquals(1, clientFrames.get());
    assertEquals(1, serverFrames.get());
    assertEquals(0, connectionFrames.get());
    assertEquals(0, initFrames.get());

    source.addToReceivedBuffer(Frame.Error.from(1, new Exception()));
    assertEquals(2, clientFrames.get());
    assertEquals(1, serverFrames.get());
    assertEquals(0, connectionFrames.get());
    assertEquals(0, initFrames.get());

    source.addToReceivedBuffer(setupFrame());
    assertEquals(2, clientFrames.get());
    assertEquals(1, serverFrames.get());
    assertEquals(0, connectionFrames.get());
    assertEquals(1, initFrames.get());

    source.addToReceivedBuffer(Frame.Error.from(0, new Exception()));
    assertEquals(2, clientFrames.get());
    assertEquals(1, serverFrames.get());
    assertEquals(1, connectionFrames.get());
    assertEquals(1, initFrames.get());
  }

  private Frame setupFrame() {
    int duration = (int) Duration.ZERO.toMillis();
    Frame setupFrame =
        Frame.Setup.from(
            SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION,
            duration,
            duration,
            "application/binary",
            "application/binary",
            PayloadImpl.EMPTY);
    return setupFrame;
  }
}
