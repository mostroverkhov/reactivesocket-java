package io.rsocket.keepalive;

import io.rsocket.exceptions.ConnectionException;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;

public class CloseOnKeepAliveTimeout implements Consumer<KeepAlives> {
  private final Consumer<Throwable> errConsumer;

  public CloseOnKeepAliveTimeout(Consumer<Throwable> errConsumer) {
    this.errConsumer = errConsumer;
  }

  @Override
  public void accept(KeepAlives keepAlives) {
    keepAlives
        .keepAliveMissing()
        .next()
        .flatMap(keepAliveMissing -> keepAlives.closeConnection().then(Mono.just(keepAliveMissing)))
        .subscribe(
            keepAliveMissing -> errConsumer.accept(keepAliveMissingError(keepAliveMissing)),
            errConsumer);
  }

  private Exception keepAliveMissingError(KeepAliveMissing keepAliveMissing) {
    String message =
        String.format(
            "Missed %d keep-alive acks with ack timeout of %d ms",
            keepAliveMissing.timeoutTicks(), keepAliveMissing.tickPeriod().toMillis());
    return new ConnectionException(message);
  }
}
