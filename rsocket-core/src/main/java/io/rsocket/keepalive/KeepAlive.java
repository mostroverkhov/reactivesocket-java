package io.rsocket.keepalive;

import java.nio.ByteBuffer;
import java.time.Duration;

public interface KeepAlive {

  class KeepAliveAvailable implements KeepAlive {
    private final ByteBuffer data;

    public KeepAliveAvailable(ByteBuffer data) {
      this.data = data;
    }

    public ByteBuffer getData() {
      return data;
    }
  }

  class KeepAliveMissing implements KeepAlive {
    private final Duration tickPeriod;
    private final int timeoutTicks;
    private final int currentTicks;

    public KeepAliveMissing(Duration tickPeriod, int timeoutTicks, int currentTicks) {
      this.tickPeriod = tickPeriod;
      this.timeoutTicks = timeoutTicks;
      this.currentTicks = currentTicks;
    }

    public Duration getTickPeriod() {
      return tickPeriod;
    }

    public int getTimeoutTicks() {
      return timeoutTicks;
    }

    public int getCurrentTicks() {
      return currentTicks;
    }
  }
}
