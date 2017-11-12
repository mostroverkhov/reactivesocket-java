package io.rsocket.internal;

import io.rsocket.DuplexConnection;
import io.rsocket.DuplexConnectionProxy;
import io.rsocket.Frame;
import io.rsocket.FrameType;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import reactor.core.publisher.Flux;

public class ZeroFramesFilterConnection extends DuplexConnectionProxy {

  private final Set<FrameType> forbiddenTypes;

  public ZeroFramesFilterConnection(DuplexConnection source, FrameType... forbiddenTypes) {
    super(source);
    assertArgs(forbiddenTypes);
    this.forbiddenTypes = toSet(forbiddenTypes);
  }

  @Override
  public Flux<Frame> receive() {
    return super.receive()
        .filter(
            frame -> {
              boolean allowed = isAllowed(frame);
              if (!allowed) {
                frame.release();
              }
              return allowed;
            });
  }

  private boolean isAllowed(Frame frame) {
    return frame.getStreamId() != 0 || !forbiddenTypes.contains(frame.getType());
  }

  private Set<FrameType> toSet(FrameType[] frameTypes) {
    Set<FrameType> res = new HashSet<>();
    Collections.addAll(res, frameTypes);
    return res;
  }

  private void assertArgs(FrameType[] frameTypes) {
    if (frameTypes == null || frameTypes.length == 0) {
      throw new IllegalArgumentException("frame types should not be empty");
    }
  }
}
