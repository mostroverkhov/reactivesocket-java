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

package com.github.mostroverkhov.rsocket.lease;

import com.github.mostroverkhov.rsocket.DuplexConnection;
import com.github.mostroverkhov.rsocket.interceptors.DuplexConnectionInterceptor;
import java.util.function.Consumer;

class LeaseGranterInterceptor implements DuplexConnectionInterceptor {
  private final LeaseContext leaseContext;
  private final LeaseManager sender;
  private final LeaseManager receiver;
  private final Consumer<LeaseConnectionRef> leaseHandle;

  public LeaseGranterInterceptor(
      LeaseContext leaseContext,
      LeaseManager sender,
      LeaseManager receiver,
      Consumer<LeaseConnectionRef> leaseHandle) {
    this.leaseContext = leaseContext;
    this.sender = sender;
    this.receiver = receiver;
    this.leaseHandle = leaseHandle;
  }

  @Override
  public DuplexConnection apply(Type type, DuplexConnection connection) {
    if (type == Type.STREAM_ZERO) {
      LeaseGranterConnection leaseGranterConnection =
          new LeaseGranterConnection(leaseContext, connection, sender, receiver);
      leaseHandle.accept(
          new LeaseConnectionRef(leaseGranterConnection, leaseGranterConnection.onClose()));
      return leaseGranterConnection;
    } else {
      return connection;
    }
  }
}
