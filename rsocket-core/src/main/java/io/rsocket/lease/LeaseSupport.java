package io.rsocket.lease;

import io.rsocket.interceptors.InterceptorFactory;
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
