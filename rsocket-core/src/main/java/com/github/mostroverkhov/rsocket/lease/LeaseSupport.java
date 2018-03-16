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

import com.github.mostroverkhov.rsocket.interceptors.InterceptorFactory;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class LeaseSupport {
  private static final LeaseContext leaseEnabled = new LeaseContext();

  public static Supplier<InterceptorFactory.InterceptorSet> missingForServer() {
    /*handles case when client requests Lease but server does not support it*/
    return () ->
        new InterceptorFactory.InterceptorSet().connection(new ServerLeaseMissingInterceptor());
  }

  public static Supplier<InterceptorFactory.InterceptorSet> forServer(
      Consumer<LeaseConnectionRef> leaseHandle) {
    return () -> {
      LeaseManager sender = new LeaseManager("server sender");
      LeaseManager receiver = new LeaseManager("server receiver");
      LeaseContext leaseContext = new LeaseContext();
      return new InterceptorFactory.InterceptorSet()
          /*requester rsocket is Lease aware*/
          .requesterRSocket(new LeaseInterceptor(leaseContext, "server requester", sender))
          /*handler rsocket is Lease aware*/
          .handlerRSocket(new LeaseInterceptor(leaseContext, "server responder", receiver))
          /*grants Lease quotas of above rsockets*/
          .connection(new LeaseGranterInterceptor(leaseContext, sender, receiver, leaseHandle))
          /*enables lease for particular connection*/
          .connection(new ServerLeaseEnablingInterceptor(leaseContext));
    };
  }

  public static Supplier<InterceptorFactory.InterceptorSet> forClient(
      Consumer<LeaseConnectionRef> leaseHandle) {
    return () -> {
      LeaseManager sender = new LeaseManager("client sender");
      LeaseManager receiver = new LeaseManager("client receiver");

      return new InterceptorFactory.InterceptorSet()
          /*requester rsocket is Lease aware*/
          .requesterRSocket(new LeaseInterceptor(leaseEnabled, "client requester", sender))
          /*handler rsocket is Lease aware*/
          .handlerRSocket(new LeaseInterceptor(leaseEnabled, "client responder", receiver))
          /*grants Lease quotas to above rsockets*/
          .connection(new LeaseGranterInterceptor(leaseEnabled, sender, receiver, leaseHandle));
    };
  }
}
