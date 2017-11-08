package io.rsocket.examples.transport.tcp.metadata;

import io.rsocket.*;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Date;

import static java.time.Duration.ofSeconds;

public class MetadataPushTest {

    private static final Logger LOGGER = LoggerFactory.getLogger("io.rsocket.examples.metadata_push");

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

                                                    @Override
                                                    public Mono<Void> metadataPush(Payload payload) {
                                                        LOGGER.info(new Date() + " server: metadata receive - " + payload.getMetadataUtf8());
                                                        return reactiveSocket
                                                                .metadataPush(new PayloadImpl("", "server metadata"));
                                                    }
                                                }))
                        .transport(TcpServerTransport.create("localhost", 7000))
                        .start()
                        .block();

        RSocket clientSocket =
                RSocketFactory.connect()
                        .acceptor(
                                () -> rsocket -> new AbstractRSocket() {
                                    @Override
                                    public Mono<Void> metadataPush(Payload payload) {
                                        LOGGER.info(new Date() + " client : metadata receive - " + payload.getMetadataUtf8());
                                        return Mono.empty();
                                    }
                                })
                        .transport(TcpClientTransport.create("localhost", 7000))
                        .start()
                        .block();

        Flux.interval(ofSeconds(1))
                .flatMap(
                        signal -> clientSocket
                                .metadataPush(new PayloadImpl("", "client metadata"))
                                .onErrorResume(
                                        err -> Mono.<Void>empty().doOnTerminate(() -> LOGGER.info("Error: " + err))))
                .subscribe();

        clientSocket.onClose().block();
    }
}
