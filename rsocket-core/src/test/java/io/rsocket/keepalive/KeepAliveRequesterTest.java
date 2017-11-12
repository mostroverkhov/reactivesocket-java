package io.rsocket.keepalive;

import static java.time.Duration.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.FrameType;
import io.rsocket.test.util.LocalDuplexConnection;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.Test;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class KeepAliveRequesterTest {

  @Test
  public void periodicRequestsSent() throws Exception {
    DirectProcessor<Frame> sender = DirectProcessor.create();
    DirectProcessor<Frame> receiver = DirectProcessor.create();
    LocalDuplexConnection conn = new LocalDuplexConnection("conn", sender, receiver);
    KeepAliveRequesterConnection receiveConn = newConn(conn);
    Mono.delay(ofMillis(100), Schedulers.elastic())
        .flatMapMany(__ -> receiveConn.receive())
        .subscribe();

    StepVerifier.create(sender.take(4))
        .expectNextMatches(this::isKeepAliveRequest)
        .expectNextMatches(this::isKeepAliveRequest)
        .expectNextMatches(this::isKeepAliveRequest)
        .expectNextMatches(this::isKeepAliveRequest)
        .expectComplete()
        .verify(ofSeconds(3));
  }

  @Test
  public void inboundKeepAlivesAreReported() throws Exception {
    DirectProcessor<Frame> sender = DirectProcessor.create();
    DirectProcessor<Frame> receiver = DirectProcessor.create();
    LocalDuplexConnection conn = new LocalDuplexConnection("conn", sender, receiver);
    KeepAliveRequesterConnection receiveConn = newConn(conn);
    receiveConn.receive().subscribe();

    Flux.interval(ofMillis(100), ofMillis(500), Schedulers.elastic())
        .take(2)
        .subscribe(__ -> receiver.onNext(responseFrame("hello")));

    StepVerifier.create(receiveConn.keepAliveAvailable().takeUntilOther(Mono.delay(Duration.ofSeconds(2))))
        .expectNextMatches(keepAlive -> isKeepAliveAvailable(keepAlive, "hello"))
        .expectNextMatches(keepAlive -> isKeepAliveAvailable(keepAlive, "hello"))
        .expectComplete()
        .verify(ofSeconds(3));
  }

  @Test
  public void missingInboundKeepAlivesAreReported() throws Exception {
    DirectProcessor<Frame> sender = DirectProcessor.create();
    DirectProcessor<Frame> receiver = DirectProcessor.create();
    LocalDuplexConnection conn = new LocalDuplexConnection("conn", sender, receiver);
    KeepAliveRequesterConnection receiveConn = newConn(conn);
    receiveConn.receive().subscribe();

    StepVerifier.create(
            receiveConn.keepAliveMissing().takeUntilOther(Mono.delay(Duration.ofSeconds(4))).take(2))
        .expectNextMatches(keepAlive -> isKeepAliveMissing(keepAlive, 3))
        .expectNextMatches(keepAlive -> isKeepAliveMissing(keepAlive, 4))
        .expectComplete()
        .verify(ofSeconds(5));
  }

  private Frame responseFrame(String data) {
    Charset utf8 = StandardCharsets.UTF_8;
    ByteBuf hello = Unpooled.wrappedBuffer(utf8.encode(data));
    return Frame.Keepalive.from(hello, false);
  }

  private boolean isKeepAliveRequest(Frame f) {
    return f.getType().equals(FrameType.KEEPALIVE) && Frame.Keepalive.hasRespondFlag(f);
  }

  private boolean isKeepAliveAvailable(KeepAlive keepAlive, String expectedData) {
    return keepAlive instanceof KeepAlive.KeepAliveAvailable
        && expectedData.equals(getData(keepAlive));
  }

  private boolean isKeepAliveMissing(KeepAlive keepAlive, int missingCount) {
    return keepAlive instanceof KeepAlive.KeepAliveMissing && missingCount == getCount(keepAlive);
  }

  private String getData(KeepAlive keepAlive) {
    return StandardCharsets.UTF_8
        .decode(((KeepAlive.KeepAliveAvailable) keepAlive).getData())
        .toString();
  }

  private int getCount(KeepAlive keepAlive) {
    return ((KeepAlive.KeepAliveMissing) keepAlive).getCurrentTicks();
  }

  private KeepAliveRequesterConnection newConn(DuplexConnection conn) {
    return new KeepAliveRequesterConnection(
        conn, ofMillis(500), 3, () -> Frame.NULL_BYTEBUFFER, err -> {});
  }
}
