package com.github.mostroverkhov.rsocket.keepalive;

import com.github.mostroverkhov.rsocket.Frame;
import com.github.mostroverkhov.rsocket.FrameType;
import com.github.mostroverkhov.rsocket.test.util.LocalDuplexConnection;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.Test;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class KeepAliveResponderTest {

  @Test
  public void respondsToKeepAliveRequest() throws Exception {
    DirectProcessor<Frame> sender = DirectProcessor.create();
    DirectProcessor<Frame> receiver = DirectProcessor.create();
    LocalDuplexConnection conn = new LocalDuplexConnection("conn", sender, receiver);
    Charset utf8 = StandardCharsets.UTF_8;
    ByteBuf hello = Unpooled.wrappedBuffer(utf8.encode("hello"));
    KeepAliveResponderConnection keepAliveResponderConnection =
        new KeepAliveResponderConnection(conn);

    keepAliveResponderConnection.receive().subscribe();

    Mono.delay(Duration.ofMillis(100), Schedulers.elastic())
        .subscribe(__ -> receiver.onNext(Frame.Keepalive.from(hello, true)));

    StepVerifier.create(sender.next())
        .expectNextMatches(
            f ->
                "hello".equals(f.getDataUtf8())
                    && f.getType().equals(FrameType.KEEPALIVE)
                    && !Frame.Keepalive.hasRespondFlag(f))
        .expectComplete()
        .verify(Duration.ofSeconds(2));
  }

  @Test
  public void ignoreKeepAliveResponse() throws Exception {
    DirectProcessor<Frame> sender = DirectProcessor.create();
    DirectProcessor<Frame> receiver = DirectProcessor.create();
    LocalDuplexConnection conn = new LocalDuplexConnection("conn", sender, receiver);
    Charset utf8 = StandardCharsets.UTF_8;
    ByteBuf hello = Unpooled.wrappedBuffer(utf8.encode("hello"));
    KeepAliveResponderConnection keepAliveResponderConnection =
        new KeepAliveResponderConnection(conn);

    keepAliveResponderConnection.receive().subscribe();

    Mono.delay(Duration.ofMillis(100), Schedulers.elastic())
        .subscribe(__ -> receiver.onNext(Frame.Keepalive.from(hello, false)));

    StepVerifier.create(sender.next().takeUntilOther(Mono.delay(Duration.ofSeconds(3))))
        .expectSubscription()
        .expectNoEvent(Duration.ofSeconds(2))
        .expectComplete()
        .verify();
  }
}
