package io.rsocket.examples.transport.tcp.keepalive;

import static io.rsocket.keepalive.KeepAlive.KeepAliveAvailable;
import static io.rsocket.keepalive.KeepAlive.KeepAliveMissing;
import static io.rsocket.plugins.DuplexConnectionInterceptor.*;

import io.rsocket.*;
import io.rsocket.keepalive.KeepAlives;
import io.rsocket.plugins.PerTypeDuplexConnectionInterceptor;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class KeepAliveClientServer {
  private static final Logger LOGGER = LoggerFactory.getLogger("io.rsocket.examples.keep_alive");

  public static void main(String[] args) {
    NettyContextCloseable nettyContextCloseable =
        RSocketFactory.receive()
            .addConnectionPlugin(
                new PerTypeDuplexConnectionInterceptor(Type.SOURCE, FlakyConnection::new))
            .acceptor(
                (setup, reactiveSocket) ->
                    Mono.just(
                        new AbstractRSocket() {
                          @Override
                          public Mono<Payload> requestResponse(Payload payload) {
                            return Mono.just(new PayloadImpl("Server Response " + new Date()));
                          }
                        }))
            .transport(TcpServerTransport.create("localhost", 7000))
            .start()
            .block();

    RSocket clientSocket =
        RSocketFactory.connect()
            .keepAlive(Duration.ofSeconds(1), 3, new FrameDataSupplier(), new KeepAlivesLogger())
            .transport(TcpClientTransport.create("localhost", 7000))
            .start()
            .block();

    clientSocket.onClose().block();
  }

  static class FlakyConnection extends DuplexConnectionProxy {
    private volatile boolean enabled = true;
    private final Disposable toggling;

    public FlakyConnection(DuplexConnection source) {
      super(source);
      toggling =
          Flux.interval(Duration.ofSeconds(4), Duration.ofSeconds(7)).subscribe(__ -> toggle());
    }

    @Override
    public Mono<Void> send(Publisher<Frame> frame) {
      return Flux.from(frame)
          .filter(f -> enabled || f.getType() != FrameType.KEEPALIVE)
          .flatMap(f -> super.send(Mono.just(f)))
          .then();
    }

    @Override
    public Mono<Void> onClose() {
      return super.onClose().doOnTerminate(toggling::dispose);
    }

    private void toggle() {
      enabled = !enabled;
    }
  }

  private static class FrameDataSupplier implements Supplier<ByteBuffer> {

    @Override
    public ByteBuffer get() {
      String date = new Date().toString();
      return msg(date);
    }
  }

  private static class KeepAlivesLogger implements Consumer<KeepAlives> {

    @Override
    public void accept(KeepAlives keepAlives) {
      Flux<KeepAliveAvailable> keepAliveAvailable = keepAlives.keepAliveAvailable();
      Flux<KeepAliveMissing> keepAliveMissing = keepAlives.keepAliveMissing();

      keepAliveAvailable.subscribe(this::logReceive, err -> LOGGER.error(err.toString()));

      keepAliveMissing.subscribe(this::logMissing, err -> LOGGER.error(err.toString()));
    }

    private void logMissing(KeepAliveMissing keepAlive) {
      LOGGER.info(String.format("Keep alive missing: %d", keepAlive.getCurrentTicks()));
    }

    private void logReceive(KeepAliveAvailable keepAlive) {
      LOGGER.info(String.format("Keep alive received: %s", msg(keepAlive.getData())));
    }
  }

  private static String msg(ByteBuffer data) {
    return StandardCharsets.UTF_8.decode(data).toString();
  }

  private static ByteBuffer msg(String data) {
    return StandardCharsets.UTF_8.encode(data);
  }
}
