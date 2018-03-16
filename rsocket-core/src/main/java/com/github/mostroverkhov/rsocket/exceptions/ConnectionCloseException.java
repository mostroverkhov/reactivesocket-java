package com.github.mostroverkhov.rsocket.exceptions;

import com.github.mostroverkhov.rsocket.frame.ErrorFrameFlyweight;

public class ConnectionCloseException extends RSocketException {

  private static final long serialVersionUID = -7659717517940756969L;

  public ConnectionCloseException(String message) {
    super(message);
  }

  public ConnectionCloseException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public int errorCode() {
    return ErrorFrameFlyweight.CONNECTION_CLOSE;
  }
}
