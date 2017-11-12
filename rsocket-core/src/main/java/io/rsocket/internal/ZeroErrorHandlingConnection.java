package io.rsocket.internal;

import io.rsocket.DuplexConnection;
import io.rsocket.DuplexConnectionProxy;
import io.rsocket.Frame;
import io.rsocket.FrameType;
import io.rsocket.exceptions.Exceptions;
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
