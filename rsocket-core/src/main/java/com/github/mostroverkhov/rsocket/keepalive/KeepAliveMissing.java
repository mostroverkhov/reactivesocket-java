/*
 * Copyright 2018 Maksym Ostroverkhov
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.github.mostroverkhov.rsocket.keepalive;

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
