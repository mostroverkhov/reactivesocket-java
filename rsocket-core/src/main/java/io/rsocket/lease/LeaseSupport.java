package io.rsocket.lease;

import io.rsocket.interceptors.InterceptorFactory;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class LeaseSupport {

  public static Supplier<InterceptorFactory.Interceptor> missingForServer() {
    /*handles case when client requests Lease but server does not support it*/
    return () ->
        new InterceptorFactory.Interceptor().connection(new ServerLeaseMissingInterceptor());
  }

  public static Supplier<InterceptorFactory.Interceptor> forServer(
      Consumer<LeaseConnectionRef> leaseHandle) {
    return () -> {
      LeaseManager sender = new LeaseManager("server sender");
      LeaseManager receiver = new LeaseManager("server receiver");
      LeaseContext leaseContext = new LeaseContext();
      return new InterceptorFactory.Interceptor()
          /*requester rsocket is Lease aware*/
          .requesterRSocket(new LeaseInterceptor("server requester", sender))
          /*handler rsocket is Lease aware*/
          .handlerRSocket(new LeaseInterceptor("server responder", receiver))
          /*grants Lease quotas of above rsockets*/
          .connection(new LeaseGranterInterceptor(sender, receiver, leaseHandle))
          /*enables lease for particular connection*/
          .connection(new ServerLeaseEnablingInterceptor(leaseContext));
    };
  }

  public static Supplier<InterceptorFactory.Interceptor> forClient(
      Consumer<LeaseConnectionRef> leaseHandle) {
    return () -> {
      LeaseManager sender = new LeaseManager("client sender");
      LeaseManager receiver = new LeaseManager("client receiver");

      return new InterceptorFactory.Interceptor()
          /*requester rsocket is Lease aware*/
          .requesterRSocket(new LeaseInterceptor("client requester", sender))
          /*handler rsocket is Lease aware*/
          .handlerRSocket(new LeaseInterceptor("client responder", receiver))
          /*grants Lease quotas to above rsockets*/
          .connection(new LeaseGranterInterceptor(sender, receiver, leaseHandle));
    };
  }
}
