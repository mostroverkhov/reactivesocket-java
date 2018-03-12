package io.rsocket.lease;

/** Shared state between set of Lease related interceptors */
class LeaseContext {
  private volatile boolean isLeaseEnabled = true;

  public LeaseContext() {}

  public LeaseContext(boolean isLeaseEnabled) {
    this.isLeaseEnabled = isLeaseEnabled;
  }

  public void leaseEnabled(boolean enabled) {
    this.isLeaseEnabled = enabled;
  }

  public boolean isLeaseEnabled() {
    return isLeaseEnabled;
  }

  @Override
  public String toString() {
    return "LeaseContext{" + "isLeaseEnabled=" + isLeaseEnabled + '}';
  }
}
