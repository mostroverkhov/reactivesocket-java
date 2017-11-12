/*
 * Copyright 2016 Netflix, Inc.
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

package io.rsocket;

import static io.rsocket.FrameType.*;
import static io.rsocket.plugins.DuplexConnectionInterceptor.Type.*;

import io.rsocket.exceptions.InvalidSetupException;
import io.rsocket.exceptions.SetupException;
import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.frame.VersionFlyweight;
import io.rsocket.internal.ConnectionDemux;
import io.rsocket.internal.ZeroFramesFilter;
import io.rsocket.keepalive.CloseOnKeepAliveTimeout;
import io.rsocket.keepalive.KeepAliveRequesterConnection;
import io.rsocket.keepalive.KeepAliveResponderConnection;
import io.rsocket.keepalive.KeepAlives;
import io.rsocket.plugins.*;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.util.PayloadImpl;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

/** Factory for creating RSocket clients and servers. */
public class RSocketFactory {
  /**
   * Creates a factory that establishes client connections to other RSockets.
   *
   * @return a client factory
   */
  public static ClientRSocketFactory connect() {
    return new ClientRSocketFactory();
  }

  /**
   * Creates a factory that receives server connections from client RSockets.
   *
   * @return a server factory.
   */
  public static ServerRSocketFactory receive() {
    return new ServerRSocketFactory();
  }

  public interface Start<T extends Closeable> {
    Mono<T> start();
  }

  public interface SetupPayload<T> {
    T setupPayload(Payload payload);
  }

  public interface Acceptor<T, A> {
    T acceptor(Supplier<A> acceptor);

    default T acceptor(A acceptor) {
      return acceptor(() -> acceptor);
    }
  }

  public interface ClientTransportAcceptor {
    Start<RSocket> transport(Supplier<ClientTransport> transport);

    default Start<RSocket> transport(ClientTransport transport) {
      return transport(() -> transport);
    }
  }

  public interface ServerTransportAcceptor {
    <T extends Closeable> Start<T> transport(Supplier<ServerTransport<T>> transport);

    default <T extends Closeable> Start<T> transport(ServerTransport<T> transport) {
      return transport(() -> transport);
    }
  }

  public interface Fragmentation<T> {
    T fragment(int mtu);
  }

  public interface ErrorConsumer<T> {
    T errorConsumer(Consumer<Throwable> errorConsumer);
  }

  public interface KeepAlive<T> {

    T keepAlive(
        Duration period,
        int periodsTimeout,
        Supplier<ByteBuffer> frameDataSupplier,
        Consumer<KeepAlives> consumer);

    default T keepAlive(Duration period, int periodsTimeout, Consumer<KeepAlives> consumer) {
      return keepAlive(period, periodsTimeout, () -> Frame.NULL_BYTEBUFFER, consumer);
    }
  }

  public interface MimeType<T> {
    T mimeType(String metadataMimeType, String dataMimeType);

    T dataMimeType(String dataMimeType);

    T metadataMimeType(String metadataMimeType);
  }

  public static class ClientRSocketFactory
      implements Acceptor<ClientTransportAcceptor, Function<RSocket, RSocket>>,
          ClientTransportAcceptor,
          KeepAlive<ClientRSocketFactory>,
          MimeType<ClientRSocketFactory>,
          Fragmentation<ClientRSocketFactory>,
          ErrorConsumer<ClientRSocketFactory>,
          SetupPayload<ClientRSocketFactory> {

    protected Supplier<Function<RSocket, RSocket>> acceptor =
        () -> rSocket -> new AbstractRSocket() {};
    protected Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
    private int mtu = 0;
    private PluginRegistry plugins = new PluginRegistry(Plugins.defaultPlugins());
    protected int flags = SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION;

    private Payload setupPayload = PayloadImpl.EMPTY;

    private Duration keepAlivePeriod = Duration.ofMillis(500);
    private int keepAlivePeriodsTimeout = 3;
    private Supplier<ByteBuffer> frameDataFactory = () -> Frame.NULL_BYTEBUFFER;
    private Consumer<KeepAlives> keepAlivesConsumer = new CloseOnKeepAliveTimeout(errorConsumer);

    private String metadataMimeType = "application/binary";
    private String dataMimeType = "application/binary";

    public ClientRSocketFactory addConnectionPlugin(DuplexConnectionInterceptor interceptor) {
      plugins.addConnectionPlugin(interceptor);
      return this;
    }

    public ClientRSocketFactory addClientPlugin(RSocketInterceptor interceptor) {
      plugins.addClientPlugin(interceptor);
      return this;
    }

    public ClientRSocketFactory addServerPlugin(RSocketInterceptor interceptor) {
      plugins.addServerPlugin(interceptor);
      return this;
    }

    @Override
    public ClientRSocketFactory keepAlive(
        Duration keepAlivePeriod,
        int keepAlivePeriodsTimeout,
        Supplier<ByteBuffer> frameDataFactory,
        Consumer<KeepAlives> keepAlivesConsumer) {
      this.keepAlivePeriod = keepAlivePeriod;
      this.keepAlivePeriodsTimeout = keepAlivePeriodsTimeout;
      this.frameDataFactory = frameDataFactory;
      this.keepAlivesConsumer = keepAlivesConsumer;
      return this;
    }

    @Override
    public ClientRSocketFactory mimeType(String metadataMimeType, String dataMimeType) {
      this.dataMimeType = dataMimeType;
      this.metadataMimeType = metadataMimeType;
      return this;
    }

    @Override
    public ClientRSocketFactory dataMimeType(String dataMimeType) {
      this.dataMimeType = dataMimeType;
      return this;
    }

    @Override
    public ClientRSocketFactory metadataMimeType(String metadataMimeType) {
      this.metadataMimeType = metadataMimeType;
      return this;
    }

    @Override
    public Start<RSocket> transport(Supplier<ClientTransport> transportClient) {
      return new StartClient(transportClient);
    }

    @Override
    public ClientTransportAcceptor acceptor(Supplier<Function<RSocket, RSocket>> acceptor) {
      this.acceptor = acceptor;
      return StartClient::new;
    }

    @Override
    public ClientRSocketFactory fragment(int mtu) {
      this.mtu = mtu;
      return this;
    }

    @Override
    public ClientRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer) {
      this.errorConsumer = errorConsumer;
      return this;
    }

    @Override
    public ClientRSocketFactory setupPayload(Payload payload) {
      this.setupPayload = payload;
      return this;
    }

    protected class StartClient implements Start<RSocket> {
      private final Supplier<ClientTransport> transportClient;

      StartClient(Supplier<ClientTransport> transportClient) {
        this.transportClient = transportClient;

        addConnectionPlugin(
            new PerTypeDuplexConnectionInterceptor(STREAM_ZERO, KeepAliveResponderConnection::new));

        addConnectionPlugin(
            new PerTypeDuplexConnectionInterceptor(
                STREAM_ZERO,
                conn -> {
                  KeepAliveRequesterConnection keepAliveRequesterConnection =
                      new KeepAliveRequesterConnection(
                          conn,
                          keepAlivePeriod,
                          keepAlivePeriodsTimeout,
                          frameDataFactory,
                          errorConsumer);
                  keepAlivesConsumer.accept(
                      new KeepAlives(
                          keepAliveRequesterConnection.keepAliveAvailable(),
                          keepAliveRequesterConnection.keepAliveMissing(),
                          keepAliveRequesterConnection.close()));

                  return keepAliveRequesterConnection;
                }));

        addConnectionPlugin(new PerTypeDuplexConnectionInterceptor(STREAM_ZERO,
                conn -> new ZeroFramesFilter(conn, KEEPALIVE, LEASE)));
      }

      @Override
      public Mono<RSocket> start() {
        return transportClient
            .get()
            .connect()
            .flatMap(
                connection -> {
                  Frame setupFrame =
                      Frame.Setup.from(
                          flags,
                          keepAlivePeriodsTimeout,
                          (int) keepAlivePeriod.toMillis() * keepAlivePeriodsTimeout,
                          metadataMimeType,
                          dataMimeType,
                          setupPayload);

                  if (mtu > 0) {
                    connection = new FragmentationDuplexConnection(connection, mtu);
                  }
                  ConnectionDemux multiplexer = new ConnectionDemux(connection, plugins);

                  RSocketRequester rSocketRequester =
                      new RSocketRequester(
                          multiplexer.asClientConnection(),
                          errorConsumer,
                          StreamIdSupplier.clientSupplier());

                  Mono<RSocket> wrappedRSocketClient =
                      Mono.just(rSocketRequester).map(plugins::applyClient);
                  DuplexConnection finalConnection = connection;

                  return wrappedRSocketClient.flatMap(
                      wrappedClientRSocket -> {
                        RSocket unwrappedServerSocket = acceptor.get().apply(wrappedClientRSocket);
                        Mono<RSocket> wrappedRSocketServer =
                            Mono.just(unwrappedServerSocket).map(plugins::applyServer);

                        return wrappedRSocketServer
                            .doOnNext(
                                rSocket ->
                                    new RSocketResponder(
                                        multiplexer.asZeroAndServerConnection(),
                                        rSocket,
                                        errorConsumer))
                            .then(finalConnection.sendOne(setupFrame))
                            .then(wrappedRSocketClient);
                      });
                });
      }
    }
  }

  public static class ServerRSocketFactory
      implements Acceptor<ServerTransportAcceptor, SocketAcceptor>,
          Fragmentation<ServerRSocketFactory>,
          ErrorConsumer<ServerRSocketFactory> {

    protected Supplier<SocketAcceptor> acceptor;
    protected Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
    private int mtu = 0;
    private PluginRegistry plugins = new PluginRegistry(Plugins.defaultPlugins());

    protected ServerRSocketFactory() {}

    public ServerRSocketFactory addConnectionPlugin(DuplexConnectionInterceptor interceptor) {
      plugins.addConnectionPlugin(interceptor);
      return this;
    }

    public ServerRSocketFactory addClientPlugin(RSocketInterceptor interceptor) {
      plugins.addClientPlugin(interceptor);
      return this;
    }

    public ServerRSocketFactory addServerPlugin(RSocketInterceptor interceptor) {
      plugins.addServerPlugin(interceptor);
      return this;
    }

    @Override
    public ServerTransportAcceptor acceptor(Supplier<SocketAcceptor> acceptor) {
      this.acceptor = acceptor;
      return ServerStart::new;
    }

    @Override
    public ServerRSocketFactory fragment(int mtu) {
      this.mtu = mtu;
      return this;
    }

    @Override
    public ServerRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer) {
      this.errorConsumer = errorConsumer;
      return this;
    }

    protected class ServerStart<T extends Closeable> implements Start<T> {
      private final Supplier<ServerTransport<T>> transportServer;

      ServerStart(Supplier<ServerTransport<T>> transportServer) {
        this.transportServer = transportServer;

        addConnectionPlugin(
            new PerTypeDuplexConnectionInterceptor(STREAM_ZERO, KeepAliveResponderConnection::new));

        addConnectionPlugin(new PerTypeDuplexConnectionInterceptor(STREAM_ZERO,
                conn -> new ZeroFramesFilter(conn, KEEPALIVE, LEASE)));
      }

      @Override
      public Mono<T> start() {
        return transportServer
            .get()
            .start(
                connection -> {
                  if (mtu > 0) {
                    connection = new FragmentationDuplexConnection(connection, mtu);
                  }
                  ConnectionDemux multiplexer = new ConnectionDemux(connection, plugins);

                  return multiplexer
                      .asInitConnection()
                      .receive()
                      .next()
                      .flatMap(setupFrame -> processSetupFrame(multiplexer, setupFrame));
                });
      }

      private Mono<Void> processSetupFrame(ConnectionDemux multiplexer, Frame setupFrame) {
        int version = Frame.Setup.version(setupFrame);
        if (version != SetupFrameFlyweight.CURRENT_VERSION) {
          InvalidSetupException error =
              new InvalidSetupException(
                  "Unsupported version " + VersionFlyweight.toString(version));
          return setupError(multiplexer, error);
        }

        RSocketRequester rSocketRequester =
            new RSocketRequester(
                multiplexer.asServerConnection(), errorConsumer, StreamIdSupplier.serverSupplier());

        Mono<RSocket> wrappedRSocketClient = Mono.just(rSocketRequester).map(plugins::applyClient);

        ConnectionSetupPayload setupPayload = ConnectionSetupPayload.create(setupFrame);

        return wrappedRSocketClient
            .flatMap(
                sender -> acceptor.get().accept(setupPayload, sender).map(plugins::applyServer))
            .map(
                handler ->
                    new RSocketResponder(
                        multiplexer.asZeroAndClientConnection(), handler, errorConsumer))
            .then();
      }

      Mono<Void> setupError(ConnectionDemux multiplexer, SetupException error) {
        return multiplexer
            .asStreamZeroConnection()
            .sendOne(Frame.Error.from(0, error))
            .then(multiplexer.close());
      }
    }
  }
}
