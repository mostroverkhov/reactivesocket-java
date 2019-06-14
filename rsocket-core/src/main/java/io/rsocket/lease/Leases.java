package io.rsocket.lease;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import jdk.internal.jline.internal.Nullable;
import reactor.core.publisher.Flux;

public class Leases<T extends LeaseStats> {
  private static final Function<?, Flux<Lease>> noopLeaseSender = leaseStats -> Flux.never();
  private static final Consumer<Flux<Lease>> noopLeaseReceiver = leases -> {};

  private Function<Optional<T>, Flux<Lease>> leaseSender =
      (Function<Optional<T>, Flux<Lease>>) noopLeaseSender;
  private Consumer<Flux<Lease>> leaseReceiver = noopLeaseReceiver;
  private Supplier<T> stats;

  public static Leases create() {
    return new Leases();
  }

  public Leases sender(Function<Optional<T>, Flux<Lease>> leaseSender) {
    this.leaseSender = leaseSender;
    return this;
  }

  public Leases receiver(Consumer<Flux<Lease>> leaseReceiver) {
    this.leaseReceiver = leaseReceiver;
    return this;
  }

  public Leases stats(Supplier<T> stats) {
    this.stats = stats;
    return this;
  }

  public Function<Optional<T>, Flux<Lease>> sender() {
    return leaseSender;
  }

  public Consumer<Flux<Lease>> receiver() {
    return leaseReceiver;
  }

  @Nullable
  public Supplier<T> stats() {
    return stats;
  }
}
