package io.rsocket.lease;

public interface LeaseStats {

  void onEvent(EventType eventType);

  enum EventType {
    ACCEPT,
    REJECT,
    TERMINATE
  }
}
