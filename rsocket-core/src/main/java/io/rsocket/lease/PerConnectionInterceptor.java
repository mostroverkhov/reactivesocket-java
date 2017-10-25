package io.rsocket.lease;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.plugins.PluginRegistry;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;

public class PerConnectionInterceptor implements DuplexConnectionInterceptor{
    private static final PluginRegistry empty = new PluginRegistry();
    private final List<Supplier<DuplexConnectionInterceptor>> interceptorsFactory;

    public PerConnectionInterceptor(Supplier<DuplexConnectionInterceptor>...interceptorsFactory) {
        this.interceptorsFactory = Arrays.asList(interceptorsFactory);
    }

    @Override
    public DuplexConnection apply(Type type, DuplexConnection duplexConnection) {
        if (type.equals(Type.SOURCE)) {
            List<DuplexConnectionInterceptor> interceptors = create();
            duplexConnection = intercept(duplexConnection, Type.SOURCE, interceptors);

            ClientServerInputMultiplexer demux = new ClientServerInputMultiplexer(duplexConnection, empty);

            DuplexConnection zero = intercept(demux.asStreamZeroConnection(), Type.STREAM_ZERO, interceptors);
            DuplexConnection client = intercept(demux.asClientConnection(), Type.CLIENT, interceptors);
            DuplexConnection server = intercept(demux.asServerConnection(), Type.SERVER, interceptors);

            return new ConnectionMux(duplexConnection,zero, client, server);
        } else {
            return duplexConnection;
        }
    }

    static class ConnectionMux implements DuplexConnection{
        private final DuplexConnection source;
        private final List<DuplexConnection> demuxed;

        public ConnectionMux(DuplexConnection source,
                             DuplexConnection...demuxed) {
            this.source = source;
            this.demuxed = Arrays.asList(demuxed);
        }

        @Override
        public Mono<Void> send(Publisher<Frame> frame) {
            return source.send(frame);
        }

        @Override
        public Flux<Frame> receive() {
            return Flux.merge(transform(demuxed, DuplexConnection::receive));
        }

        @Override
        public double availability() {
            return source.availability();
        }

        @Override
        public Mono<Void> close() {
            return whenCompleted(DuplexConnection::close);
        }

        @Override
        public Mono<Void> onClose() {
            return whenCompleted(DuplexConnection::onClose);
        }

        Mono<Void> whenCompleted(Function<DuplexConnection, Mono<Void>> mapper) {
            return Flux.fromIterable(demuxed).flatMap(mapper).then();
        }

        static  <T> List<T> transform(List<DuplexConnection> col, Function<DuplexConnection, T> mapper) {
            return col.stream().map(mapper).collect(toList());
        }

    }

    List<DuplexConnectionInterceptor> create() {
        return interceptorsFactory
                .stream()
                .map(Supplier::get)
                .collect(toList());
    }

    static DuplexConnection intercept(DuplexConnection source,
                                      Type type,
                                      List<DuplexConnectionInterceptor> interceptors) {
        for (DuplexConnectionInterceptor interceptor : interceptors) {
            source = interceptor.apply(type, source);
        }
        return source;
    }
}
