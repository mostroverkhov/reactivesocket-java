package io.rsocket.keepalive;

import java.time.Duration;

public class KeepAliveMissing {
  private final Duration tickPeriod;
  private final int timeoutTicks;

  public KeepAliveMissing(Duration tickPeriod, int timeoutTicks) {
    this.tickPeriod = tickPeriod;
    this.timeoutTicks = timeoutTicks;
  }

  public Duration tickPeriod() {
    return tickPeriod;
  }

  public int timeoutTicks() {
    return timeoutTicks;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    KeepAliveMissing that = (KeepAliveMissing) o;

    if (timeoutTicks != that.timeoutTicks) return false;
    return tickPeriod.equals(that.tickPeriod);
  }

  @Override
  public int hashCode() {
    int result = tickPeriod.hashCode();
    result = 31 * result + timeoutTicks;
    return result;
  }
}
