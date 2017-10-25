package io.rsocket;

import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.lease.LeaseConnectionRef;
import io.rsocket.lease.LeaseInterceptor;
import io.rsocket.lease.PerConnectionInterceptor;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class CoolRSocketFactory extends RSocketFactory {

    public static CoolClientRSocketFactory connect() {
        return new CoolClientRSocketFactory();
    }

    public static CoolRSocketServerFactory receive() {
        return new CoolRSocketServerFactory();
    }

    public interface Leasing<T> {
      T enableLease(Consumer<LeaseConnectionRef> leaseControlConsumer);

      T disableLease();
    }

    public static class CoolClientRSocketFactory extends ClientRSocketFactory implements Leasing<CoolClientRSocketFactory>{

        private Optional<Consumer<LeaseConnectionRef>> leaseConsumer = Optional.empty();

        @Override
        public CoolClientRSocketFactory enableLease(Consumer<LeaseConnectionRef> leaseControlConsumer) {
            this.leaseConsumer = Optional.of(leaseControlConsumer);
            flags |= SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE;
            return this;
        }

        @Override
        public CoolClientRSocketFactory disableLease() {
            this.leaseConsumer = Optional.empty();
            flags &= ~SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE;
            return this;
        }

        @Override
        public Start<RSocket> transport(Supplier<ClientTransport> transportClient) {
            return new CoolStartClient(transportClient);
        }

        @Override
        public ClientTransportAcceptor acceptor(Supplier<Function<RSocket, RSocket>> acceptor) {
            this.acceptor = acceptor;
            return CoolStartClient::new;
        }

        class CoolStartClient extends StartClient {
            CoolStartClient(Supplier<ClientTransport> transportClient) {
                super(transportClient);
                leaseConsumer.ifPresent(
                        leaseConsumer ->
                                addConnectionPlugin(new PerConnectionInterceptor(
                                        () -> LeaseInterceptor.ofClient(errorConsumer, leaseConsumer))));
            }
        }
    }

    public static class CoolRSocketServerFactory extends ServerRSocketFactory implements Leasing<CoolRSocketServerFactory>{

        private Optional<Consumer<LeaseConnectionRef>> leaseControlConsumer = Optional.empty();

        protected CoolRSocketServerFactory() {
        }

        @Override
        public CoolRSocketServerFactory enableLease(Consumer<LeaseConnectionRef> leaseControlConsumer) {
            this.leaseControlConsumer = Optional.of(leaseControlConsumer);
            return this;
        }

        @Override
        public CoolRSocketServerFactory disableLease() {
            this.leaseControlConsumer = Optional.empty();
            return this;
        }

        @Override
        public ServerTransportAcceptor acceptor(Supplier<SocketAcceptor> acceptor) {
            this.acceptor = acceptor;
            return CoolServerStart::new;
        }

        class CoolServerStart<T extends Closeable> extends ServerStart<T> {
            CoolServerStart(Supplier<ServerTransport<T>> transportServer) {
                super(transportServer);

                if (leaseControlConsumer.isPresent()) {
                    addConnectionPlugin(
                            new PerConnectionInterceptor(() -> LeaseInterceptor.ofServer(
                                    errorConsumer,
                                    leaseControlConsumer.get(),
                                    true)));
                } else {
                    addConnectionPlugin(
                            new PerConnectionInterceptor(() ->
                                    LeaseInterceptor.ofServer(
                                            errorConsumer,
                                            connRef -> {},
                                            false)));
                }
            }
        }
    }
}
