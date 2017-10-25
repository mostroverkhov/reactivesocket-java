package io.rsocket.lease;

import static io.rsocket.FrameType.*;
import static java.util.Arrays.*;
import static java.util.Collections.*;

import io.rsocket.DuplexConnection;
import io.rsocket.DuplexConnectionProxy;
import io.rsocket.Frame;
import io.rsocket.FrameType;
import io.rsocket.exceptions.NoLeaseException;
import io.rsocket.exceptions.UnsupportedSetupException;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

public class LeaseInterceptor implements DuplexConnectionInterceptor {

  private final LeaseGranterFactory leaseGranterFactory;
  private final Map<Type, List<LeaseConnectionFactory>> factories;
  private final Consumer<LeaseConnectionRef> leaseConsumer;
  private final Consumer<Throwable> errConsumer;
  private Args args;

  LeaseInterceptor(
      LeaseGranterFactory leaseGranterFactory,
      Map<Type, List<LeaseConnectionFactory>> connFactories,
      Consumer<LeaseConnectionRef> leaseConsumer,
      Consumer<Throwable> errConsumer) {
    this.leaseGranterFactory = leaseGranterFactory;
    this.factories = connFactories;
    this.leaseConsumer = leaseConsumer;
    this.errConsumer = errConsumer;
  }

  @Override
  public DuplexConnection apply(Type type, DuplexConnection duplexConnection) {
    if (args == null) {
      Mono<Void> onClose = duplexConnection.onClose();
      LeaseManager reqLeaseManager = new LeaseManager("requester", onClose);
      LeaseManager respLeaseManager = new LeaseManager("responder", onClose);
      LeaseGranter leaseGranter =
          leaseGranterFactory.apply(
              duplexConnection, reqLeaseManager, respLeaseManager, errConsumer);
      LeaseConnectionRef connectionRef = new LeaseConnectionRef(leaseGranter, onClose);

      leaseConsumer.accept(connectionRef);

      args =
          new Args(
              reqLeaseManager, respLeaseManager, leaseGranter.grantedLeasesReceiver(), errConsumer);
    }
    List<LeaseConnectionFactory> connectionFactories =
        factories.getOrDefault(type, singletonList((conn, args) -> conn));
    for (LeaseConnectionFactory factory : connectionFactories) {
      duplexConnection = factory.apply(duplexConnection, args);
    }
    return duplexConnection;
  }

  public static LeaseInterceptor ofServer(
      Consumer<Throwable> errCons,
      Consumer<LeaseConnectionRef> leaseConsumer,
      boolean leaseEnabled) {
    String tag = Classifier.Server.name();
    Context ctx = new Context();
    return of(
        errCons,
        leaseConsumer,
        LeaseGranter::ofServer,
        singletonList(
            (conn, args) ->
                new RequestOutboundConnection(tag, conn, ctx, args.getRequesterLeaseManager())),
        singletonList(
            (conn, args) ->
                new ServerLeaseSetupConnection(conn, ctx, leaseEnabled, args.getErrConsumer())),
        asList(
            (conn, args) ->
                new RequestInboundConnection(tag, conn, ctx, args.getResponderLeaseManager()),
            (conn, args) -> new LeaseInConnection(tag, conn, ctx, args.getLeaseConsumer())),
        emptyList());
  }

  public static LeaseInterceptor ofClient(
      Consumer<Throwable> errCons, Consumer<LeaseConnectionRef> leaseConsumer) {
    Context ctx = new Context(true);
    String tag = Classifier.Client.name();
    return of(
        errCons,
        leaseConsumer,
        LeaseGranter::ofClient,
        singletonList(
            (conn, args) ->
                new RequestOutboundConnection(tag, conn, ctx, args.getRequesterLeaseManager())),
        emptyList(),
        singletonList(
            (conn, args) -> new LeaseInConnection(tag, conn, ctx, args.getLeaseConsumer())),
        singletonList(
            (conn, args) ->
                new RequestInboundConnection(tag, conn, ctx, args.getResponderLeaseManager())));
  }

  private static LeaseInterceptor of(
      Consumer<Throwable> errCons,
      Consumer<LeaseConnectionRef> leaseConsumer,
      LeaseGranterFactory leaseGranterFactory,
      List<LeaseConnectionFactory> source,
      List<LeaseConnectionFactory> setup,
      List<LeaseConnectionFactory> client,
      List<LeaseConnectionFactory> server) {

    Map<Type, List<LeaseConnectionFactory>> res = new HashMap<>();
    res.put(Type.SOURCE, source);
    res.put(Type.STREAM_ZERO, setup);
    res.put(Type.CLIENT, client);
    res.put(Type.SERVER, server);

    return new LeaseInterceptor(leaseGranterFactory, res, leaseConsumer, errCons);
  }

  abstract static class LeaseConnection extends DuplexConnectionProxy {

    private final Context context;
    private final String tag;

    public LeaseConnection(DuplexConnection source, Context context, String tag) {
      super(source);
      this.context = context;
      this.tag = tag;
    }

    public void enableLease() {
      context.enable();
    }

    public boolean isLeaseEnabled() {
      return context.isLeaseEnabled();
    }

    protected String getTag() {
      return tag;
    }
  }

  static class ServerLeaseSetupConnection extends LeaseConnection {
    private final UnicastProcessor<Frame> setupFrames = UnicastProcessor.create();
    private final boolean serverLeaseEnabled;
    private final Consumer<Throwable> errConsumer;
    private volatile Disposable leaseErrorSubs = () -> {};

    public ServerLeaseSetupConnection(
        DuplexConnection setupConnection,
        Context context,
        boolean serverLeaseEnabled,
        Consumer<Throwable> errConsumer) {
      super(setupConnection, context, "server");
      this.serverLeaseEnabled = serverLeaseEnabled;
      this.errConsumer = errConsumer;
    }

    @Override
    public Flux<Frame> receive() {
      super.receive()
          .next()
          .subscribe(
              frame -> {
                boolean clientLeaseEnabled = isSetup(frame) && Frame.Setup.supportsLease(frame);
                if (clientLeaseEnabled && !serverLeaseEnabled) {
                  UnsupportedSetupException error =
                      new UnsupportedSetupException("Server does not support lease");
                  leaseErrorSubs =
                      sendOne(Frame.Error.from(0, error))
                          .then(close())
                          .doOnError(errConsumer)
                          .onErrorResume(err -> Mono.empty())
                          .subscribe();
                } else {
                  if (clientLeaseEnabled) {
                    enableLease();
                  }
                  setupFrames.onNext(frame);
                }
              },
              setupFrames::onError);

      return setupFrames;
    }

    @Override
    public Mono<Void> onClose() {
      return super.onClose().then(Mono.fromRunnable(() -> leaseErrorSubs.dispose()));
    }
  }

  abstract static class RequestConnection extends LeaseConnection {
    protected final LeaseManager leaseManager;

    public RequestConnection(
        String tag, DuplexConnection source, Context context, LeaseManager leaseManager) {
      super(source, context, tag);
      this.leaseManager = leaseManager;
    }

    @Override
    public double availability() {
      LeaseImpl lease = leaseManager.getLease();
      return lease.isValid()
          ? lease.getAllowedRequests() / (double) lease.getStartingAllowedRequests()
          : 0.0;
    }

    protected Frame handleFrame(Frame f) {
      if (isRequest(f)) {
        Lease lease = leaseManager.getLease();
        if (lease.isValid()) {
          leaseManager.useLease();
          return f;
        } else {
          throw new NoLeaseException(lease, getTag());
        }
      } else {
        return f;
      }
    }
  }

  static class RequestOutboundConnection extends RequestConnection {

    public RequestOutboundConnection(
        String tag,
        DuplexConnection sourceConnection,
        Context context,
        LeaseManager requesterLeaseManager) {
      super(tag, sourceConnection, context, requesterLeaseManager);
    }

    @Override
    public Mono<Void> send(Publisher<Frame> frames) {
      return Flux.from(frames)
          .concatMap(
              frame -> {
                Function<Frame, Frame> f =
                    isLeaseEnabled() ? this::handleFrame : Function.identity();
                return mono(f).apply(frame);
              })
          .then();
    }

    private Function<Frame, Mono<Frame>> mono(Function<Frame, Frame> f) {
      return frame -> {
        try {
          return Mono.just(f.apply(frame));
        } catch (Throwable err) {
          return Mono.error(err);
        }
      };
    }
  }

  static class RequestInboundConnection extends RequestConnection {
    public RequestInboundConnection(
        String classifier,
        DuplexConnection peerConnection,
        Context context,
        LeaseManager responderLeaseManager) {
      super(classifier, peerConnection, context, responderLeaseManager);
    }

    @Override
    public Flux<Frame> receive() {
      return super.receive().map(frame -> isLeaseEnabled() ? handleFrame(frame) : frame);
    }
  }

  static class LeaseInConnection extends LeaseConnection {

    private final Consumer<Lease> leaseConsumer;

    public LeaseInConnection(
        String classifier,
        DuplexConnection source,
        Context context,
        Consumer<Lease> leaseConsumer) {
      super(source, context, classifier);
      this.leaseConsumer = leaseConsumer;
    }

    @Override
    public Flux<Frame> receive() {
      return super.receive()
          .doOnNext(
              frame -> {
                if (isLeaseEnabled() && isLease(frame)) {
                  leaseConsumer.accept(new LeaseImpl(frame));
                }
              });
    }

    private static boolean isLease(Frame frame) {
      return frame.getType().equals(LEASE);
    }
  }

  static class Context {
    private boolean leaseEnabled;

    public Context(boolean leaseEnabled) {
      this.leaseEnabled = leaseEnabled;
    }

    public Context() {
      this(false);
    }

    public boolean isLeaseEnabled() {
      return leaseEnabled;
    }

    public void enable() {
      leaseEnabled = true;
    }
  }

  @FunctionalInterface
  private interface LeaseConnectionFactory
      extends BiFunction<DuplexConnection, Args, DuplexConnection> {}

  @FunctionalInterface
  interface LeaseGranterFactory {
    LeaseGranter apply(
        DuplexConnection senderConnection,
        LeaseManager requesterLeaseManager,
        LeaseManager responderLeaseManager,
        Consumer<Throwable> errorConsumer);
  }

  private static class Args {
    private final LeaseManager requesterLeaseManager;
    private final LeaseManager responderLeaseManager;
    private final Consumer<Lease> leaseConsumer;
    private Consumer<Throwable> errConsumer;

    public Args(
        LeaseManager requesterLeaseManager,
        LeaseManager responderLeaseManager,
        Consumer<Lease> leaseConsumer,
        Consumer<Throwable> errConsumer) {
      this.requesterLeaseManager = requesterLeaseManager;
      this.responderLeaseManager = responderLeaseManager;
      this.leaseConsumer = leaseConsumer;
      this.errConsumer = errConsumer;
    }

    public Consumer<Lease> getLeaseConsumer() {
      return leaseConsumer;
    }

    public LeaseManager getRequesterLeaseManager() {
      return requesterLeaseManager;
    }

    public LeaseManager getResponderLeaseManager() {
      return responderLeaseManager;
    }

    public Consumer<Throwable> getErrConsumer() {
      return errConsumer;
    }
  }

  private static boolean isRequest(Frame frame) {
    return requests.contains(frame.getType());
  }

  private static boolean isSetup(Frame frame) {
    return frame.getType().equals(SETUP);
  }

  private static final Set<FrameType> requests =
      new HashSet<>(asList(REQUEST_CHANNEL, REQUEST_RESPONSE, REQUEST_STREAM, FIRE_AND_FORGET));

  private enum Classifier {
    Client,
    Server
  }
}
