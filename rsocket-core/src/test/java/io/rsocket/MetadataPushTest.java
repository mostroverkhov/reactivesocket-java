package io.rsocket;

import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.plugins.PluginRegistry;
import io.rsocket.test.util.LocalDuplexConnection;
import io.rsocket.util.PayloadImpl;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.function.Function;

public class MetadataPushTest {

    @Test(timeout = 2000)
    public void testClientResponderMetadataPush() throws Exception {
        testResponderMetadataPush("client", ClientServerInputMultiplexer::asServerConnection);
    }

    @Test(timeout = 2000)
    public void testServerResponderMetadataPush() throws Exception {
        testResponderMetadataPush("server", ClientServerInputMultiplexer::asClientConnection);
    }

    private void testResponderMetadataPush(String connName, Function<ClientServerInputMultiplexer,
            DuplexConnection> connF) throws Exception {
        DirectProcessor<Frame> sender = DirectProcessor.create();
        DirectProcessor<Frame> receiver = DirectProcessor.create();
        DuplexConnection conn =
                connF.apply(new ClientServerInputMultiplexer(
                        new LocalDuplexConnection(connName, sender, receiver),
                        new PluginRegistry()));

        String metadata = "metadata";
        MonoProcessor<Void> completeSignal = MonoProcessor.create();
        RSocketServer responder = new RSocketServer(conn, new AbstractRSocket() {
            @Override
            public Mono<Void> metadataPush(Payload payload) {
                try {
                    Assert.assertEquals(metadata, payload.getMetadataUtf8());
                    completeSignal.onComplete();
                } catch (Throwable e) {
                    completeSignal.onError(e);
                }
                return Mono.empty();
            }
        }, err -> {
        });
        Payload metadataPayload = new PayloadImpl("", metadata);
        receiver.onNext(Frame.Request
                .from(0, FrameType.METADATA_PUSH, metadataPayload, 1));
        completeSignal.block();
    }
}
