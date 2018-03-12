package io.rsocket.lease;

import static io.rsocket.FrameType.SETUP;

import io.rsocket.DuplexConnection;
import io.rsocket.DuplexConnectionProxy;
import io.rsocket.Frame;
import io.rsocket.exceptions.UnsupportedSetupException;
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
              if (frame.getType().equals(SETUP) && Frame.Setup.supportsLease(frame)) {
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
