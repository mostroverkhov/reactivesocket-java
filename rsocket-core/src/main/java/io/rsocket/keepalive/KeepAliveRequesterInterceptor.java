package io.rsocket.keepalive;

import io.rsocket.DuplexConnection;
import io.rsocket.interceptors.DuplexConnectionInterceptor;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class KeepAliveRequesterInterceptor implements DuplexConnectionInterceptor {
  private final Duration period;
  private final int ticksTimeout;
  private final Supplier<ByteBuffer> payloadSupplier;
  private final Consumer<Throwable> errorConsumer;
  private final Consumer<KeepAlives> keepAlivesConsumer;

  public KeepAliveRequesterInterceptor(
      Duration period,
      int ticksTimeout,
      Supplier<ByteBuffer> payloadSupplier,
      Consumer<Throwable> errorConsumer,
      Consumer<KeepAlives> keepAlivesConsumer) {
    this.period = period;
    this.ticksTimeout = ticksTimeout;
    this.payloadSupplier = payloadSupplier;
    this.errorConsumer = errorConsumer;
    this.keepAlivesConsumer = keepAlivesConsumer;
  }

  @Override
  public DuplexConnection apply(Type type, DuplexConnection connection) {
    if (type == Type.STREAM_ZERO) {
      KeepAliveRequesterConnection keepAliveRequesterConnection =
          new KeepAliveRequesterConnection(
              connection, period, ticksTimeout, payloadSupplier, errorConsumer);
      keepAlivesConsumer.accept(
          new KeepAlives(
              keepAliveRequesterConnection.keepAliveAvailable(),
              keepAliveRequesterConnection.keepAliveMissing(),
              keepAliveRequesterConnection.close()));

      return keepAliveRequesterConnection;
    } else {
      return connection;
    }
  }
}
