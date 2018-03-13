package io.rsocket.lease;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.RSocketProxy;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class LeaseRSocket extends RSocketProxy {
  private static final LeaseContext alwaysEnabled = new LeaseContext();
  private final LeaseContext leaseContext;
  private final String tag;
  private final LeaseManager leaseManager;

  public LeaseRSocket(RSocket source, String tag, LeaseManager leaseManager) {
    this(alwaysEnabled, source, tag, leaseManager);
  }

  public LeaseRSocket(
      LeaseContext leaseContext, RSocket source, String tag, LeaseManager leaseManager) {
    super(source);
    this.leaseContext = leaseContext;
    this.tag = tag;
    this.leaseManager = leaseManager;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return request(super.fireAndForget(payload), Mono::error);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return request(super.requestResponse(payload), Mono::error);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return request(super.requestStream(payload), Flux::error);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return request(super.requestChannel(payloads), Flux::error);
  }

  @Override
  public double availability() {
    return Math.min(super.availability(), leaseManager.availability());
  }

  private <K, T extends Publisher<K>> T request(
      T actual, Function<? super Throwable, ? extends T> error) {
    if (isEnabled()) {
      try {
        leaseManager.useLease();
        return actual;
      } catch (Exception e) {
        return error.apply(e);
      }
    } else {
      return actual;
    }
  }

  @Override
  public String toString() {
    return "LeaseRSocket{"
        + "leaseContext="
        + leaseContext
        + ", tag='"
        + tag
        + '\''
        + ", leaseManager="
        + leaseManager
        + '}';
  }

  private boolean isEnabled() {
    return leaseContext.isLeaseEnabled();
  }
}
