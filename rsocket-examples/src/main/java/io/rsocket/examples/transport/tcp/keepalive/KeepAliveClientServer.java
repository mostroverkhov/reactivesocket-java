package io.rsocket.examples.transport.tcp.keepalive;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.keepalive.KeepAlives;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.rsocket.keepalive.KeepAlive.KeepAliveAvailable;
import static io.rsocket.keepalive.KeepAlive.KeepAliveMissing;

public class KeepAliveClientServer {
    private static final Logger LOGGER = LoggerFactory.getLogger("io.rsocket.examples.keep_alive");

    public static void main(String[] args) {
        NettyContextCloseable nettyContextCloseable =
                RSocketFactory.receive()
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
                        .keepAlive(Duration.ofSeconds(2), 3,
                                new FrameDataSupplier(),
                                new KeepAlivesLogger())
                        .transport(TcpClientTransport.create("localhost", 7000))
                        .start()
                        .block();

        clientSocket.onClose().block();
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

            keepAliveAvailable.subscribe(keepAlive -> {
                        LOGGER.info(String.format("Keep alive received: %s", msg(keepAlive.getData())));
                    },
                    err -> LOGGER.error(err.toString()));

            keepAliveMissing.subscribe(
                    keepAlive -> LOGGER.info(String.format("Keep alive missing: %d", keepAlive.getCurrentTicks())),
                    err -> LOGGER.error(err.toString()));
        }
    }

    private static String msg(ByteBuffer data) {
        return StandardCharsets.UTF_8.decode(data).toString();
    }

    private static ByteBuffer msg(String data) {
        return StandardCharsets.UTF_8.encode(data);
    }
}
