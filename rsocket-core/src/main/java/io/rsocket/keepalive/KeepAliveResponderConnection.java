package io.rsocket.keepalive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.DuplexConnection;
import io.rsocket.DuplexConnectionProxy;
import io.rsocket.Frame;
import io.rsocket.FrameType;
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
