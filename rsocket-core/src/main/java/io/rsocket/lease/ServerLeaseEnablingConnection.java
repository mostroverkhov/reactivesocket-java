package io.rsocket.lease;

import static io.rsocket.FrameType.SETUP;

import io.rsocket.DuplexConnection;
import io.rsocket.DuplexConnectionProxy;
import io.rsocket.Frame;
import reactor.core.publisher.Flux;

class ServerLeaseEnablingConnection extends DuplexConnectionProxy {
  private final LeaseContext leaseContext;

  public ServerLeaseEnablingConnection(
      DuplexConnection setupConnection, LeaseContext leaseContext) {
    super(setupConnection);
    this.leaseContext = leaseContext;
  }

  @Override
  public Flux<Frame> receive() {
    return super.receive()
        .doOnNext(
            f -> {
              boolean enabled = f.getType().equals(SETUP) && Frame.Setup.supportsLease(f);
              leaseContext.leaseEnabled(enabled);
            });
  }
}
