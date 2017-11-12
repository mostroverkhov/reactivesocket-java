package io.rsocket.internal;

import io.rsocket.Frame;
import io.rsocket.FrameType;
import io.rsocket.test.util.LocalDuplexConnection;
import io.rsocket.test.util.TestDuplexConnection;
import org.junit.Test;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;

public class ZeroFramesFilterConnectionTest {

    @Test(expected = IllegalArgumentException.class)
    public void throwOnEmptyFrameTypes() throws Exception {
        new ZeroFramesFilterConnection(new TestDuplexConnection());
    }

    @Test
    public void filterAllowsNonZeroFrames() throws Exception {
        ZeroFramesFilterConnection filter = send(Frame.Error.from(1, new RuntimeException()), FrameType.ERROR);

        StepVerifier.create(filter.receive().count())
                .expectNext(1L)
                .expectComplete()
                .verify(Duration.ofSeconds(2));
    }

    @Test
    public void filterAllowsNonForbiddenFrames() throws Exception {
        ZeroFramesFilterConnection filter = send(Frame.Error.from(0, new RuntimeException()), FrameType.KEEPALIVE);

        StepVerifier.create(filter.receive().count())
                .expectNext(1L)
                .expectComplete()
                .verify(Duration.ofSeconds(2));
    }

    @Test
    public void filterDisallowsForbiddenFrames() throws Exception {
        ZeroFramesFilterConnection filter = send(Frame.Error.from(0, new RuntimeException()), FrameType.ERROR);

        StepVerifier.create(filter.receive().count())
                .expectNext(0L)
                .expectComplete()
                .verify(Duration.ofSeconds(2));
    }

    private ZeroFramesFilterConnection send(Frame sendFrame, FrameType... forbid) {
        DirectProcessor<Frame> send = DirectProcessor.create();
        DirectProcessor<Frame> receive = DirectProcessor.create();
        ZeroFramesFilterConnection filter = new ZeroFramesFilterConnection(new LocalDuplexConnection("conn", send, receive), forbid);

        Mono.delay(Duration.ofMillis(100))
                .subscribeOn(Schedulers.elastic())
                .subscribe(__ -> {
                    receive.onNext(sendFrame);
                    receive.onComplete();
                });
        return filter;
    }
}
