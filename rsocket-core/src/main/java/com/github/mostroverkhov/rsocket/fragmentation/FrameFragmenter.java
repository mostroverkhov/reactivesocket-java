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

package com.github.mostroverkhov.rsocket.fragmentation;

import com.github.mostroverkhov.rsocket.Frame;
import com.github.mostroverkhov.rsocket.FrameType;
import com.github.mostroverkhov.rsocket.frame.FrameHeaderFlyweight;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

public class FrameFragmenter {
  private final int mtu;

  public FrameFragmenter(int mtu) {
    this.mtu = mtu;
  }

  public boolean shouldFragment(Frame frame) {
    return isFragmentableFrame(frame.getType())
        && FrameHeaderFlyweight.payloadLength(frame.content()) > mtu;
  }

  private boolean isFragmentableFrame(FrameType type) {
    switch (type) {
      case FIRE_AND_FORGET:
      case REQUEST_STREAM:
      case REQUEST_CHANNEL:
      case REQUEST_RESPONSE:
      case PAYLOAD:
      case NEXT_COMPLETE:
      case METADATA_PUSH:
        return true;
      default:
        return false;
    }
  }

  public Flux<Frame> fragment(Frame frame) {

    return Flux.generate(new FragmentGenerator(frame));
  }

  private class FragmentGenerator implements Consumer<SynchronousSink<Frame>> {
    private final Frame frame;
    private final int streamId;
    private final FrameType frameType;
    private final int flags;

    private ByteBuf data;
    private @Nullable ByteBuf metadata;

    public FragmentGenerator(Frame frame) {
      this.frame = frame.retain();
      this.streamId = frame.getStreamId();
      this.frameType = frame.getType();
      this.flags = frame.flags() & ~FrameHeaderFlyweight.FLAGS_M;
      metadata =
          frame.hasMetadata() ? FrameHeaderFlyweight.sliceFrameMetadata(frame.content()) : null;
      data = FrameHeaderFlyweight.sliceFrameData(frame.content());
    }

    @Override
    public void accept(SynchronousSink<Frame> sink) {
      final int dataLength = data.readableBytes();

      if (metadata != null) {
        final int metadataLength = metadata.readableBytes();

        if (metadataLength > mtu) {
          sink.next(
              Frame.PayloadFrame.from(
                  streamId,
                  frameType,
                  metadata.readSlice(mtu),
                  Unpooled.EMPTY_BUFFER,
                  flags | FrameHeaderFlyweight.FLAGS_M | FrameHeaderFlyweight.FLAGS_F));
        } else {
          if (dataLength > mtu - metadataLength) {
            sink.next(
                Frame.PayloadFrame.from(
                    streamId,
                    frameType,
                    metadata.readSlice(metadataLength),
                    data.readSlice(mtu - metadataLength),
                    flags | FrameHeaderFlyweight.FLAGS_M | FrameHeaderFlyweight.FLAGS_F));
          } else {
            sink.next(
                Frame.PayloadFrame.from(
                    streamId,
                    frameType,
                    metadata.readSlice(metadataLength),
                    data.readSlice(dataLength),
                    flags | FrameHeaderFlyweight.FLAGS_M));
            frame.release();
            sink.complete();
          }
        }
      } else {
        if (dataLength > mtu) {
          sink.next(
              Frame.PayloadFrame.from(
                  streamId,
                  frameType,
                  Unpooled.EMPTY_BUFFER,
                  data.readSlice(mtu),
                  flags | FrameHeaderFlyweight.FLAGS_F));
        } else {
          sink.next(
              Frame.PayloadFrame.from(
                  streamId, frameType, Unpooled.EMPTY_BUFFER, data.readSlice(dataLength), flags));
          frame.release();
          sink.complete();
        }
      }
    }
  }
}
