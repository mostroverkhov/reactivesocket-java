package io.rsocket.lease;

import static io.rsocket.FrameType.*;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

public class LeaseInterceptor implements DuplexConnectionInterceptor {
  private static final String CLIENT = "Client";
  private static final String SERVER = "Server";

  private final LeaseGranterFactory leaseGranterFactory;
  private final Map<Type, LeaseConnectionFactory> factories;
  private final Consumer<LeaseConnectionRef> leaseConsumer;
  private final Consumer<Throwable> errConsumer;
  private Args args;

  LeaseInterceptor(
      LeaseGranterFactory leaseGranterFactory,
      Map<Type, LeaseConnectionFactory> connFactories,
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
      LeaseConnectionRef connectionRef =
          new LeaseConnectionRef(leaseGranter, duplexConnection.onClose());

      leaseConsumer.accept(connectionRef);

      args = new Args(reqLeaseManager, respLeaseManager, leaseGranter.grantedLeasesReceiver());
    }
    LeaseConnectionFactory connectionFactory = factories.getOrDefault(type, (conn, args) -> conn);
    return connectionFactory.apply(duplexConnection, args);
  }

  private static LeaseInterceptor of(
      Consumer<Throwable> errCons,
      Consumer<LeaseConnectionRef> leaseConsumer,
      LeaseGranterFactory leaseGranterFactory,
      LeaseConnectionFactory source,
      LeaseConnectionFactory setup,
      LeaseConnectionFactory client,
      LeaseConnectionFactory server) {

    Map<Type, LeaseConnectionFactory> res = new HashMap<>();
    res.put(Type.SOURCE, source);
    res.put(Type.STREAM_ZERO, setup);
    res.put(Type.CLIENT, client);
    res.put(Type.SERVER, server);

    return new LeaseInterceptor(leaseGranterFactory, res, leaseConsumer, errCons);
  }

  public static LeaseInterceptor ofServer(
      Consumer<Throwable> errCons,
      Consumer<LeaseConnectionRef> leaseConsumer,
      boolean leaseEnabled) {
    String classifier = SERVER;
    Context ctx = new Context();
    return of(
        errCons,
        leaseConsumer,
        LeaseGranter::ofServer,
        (conn, args) -> new LeaseInConnection(classifier,conn, ctx, args.getLeaseConsumer()),
        (conn, args) -> new ServerLeaseSetupConnection(conn, ctx, leaseEnabled),
        (conn, args) -> new RequestInboundConnection(classifier,conn, ctx, args.getResponderLeaseManager()),
        (conn, args) -> new RequestOutboundConnection(classifier,conn, ctx, args.getRequesterLeaseManager()));
  }

  public static LeaseInterceptor ofClient(
      Consumer<Throwable> errCons, Consumer<LeaseConnectionRef> leaseConsumer) {
    Context ctx = new Context(true);
    String classifier = CLIENT;
    return of(
        errCons,
        leaseConsumer,
        LeaseGranter::ofClient,
        (conn, args) -> new LeaseInConnection(classifier,conn, ctx, args.getLeaseConsumer()),
        (conn, args) -> conn,
        (conn, args) -> new RequestOutboundConnection(classifier,conn, ctx, args.getRequesterLeaseManager()),
        (conn, args) -> new RequestInboundConnection(classifier,conn, ctx, args.getResponderLeaseManager()));
  }

  abstract static class LeaseConnection extends DuplexConnectionProxy {

    private final Context context;
    private final String classifier;

    public LeaseConnection(DuplexConnection source, Context context, String classifier) {
      super(source);
      this.context = context;
      this.classifier = classifier;
    }

    public void enableLease() {
      context.enable();
    }

    public boolean isLeaseEnabled() {
      return context.isLeaseEnabled();
    }

    protected String getClassifier() {
      return classifier;
    }
  }

  static class ServerLeaseSetupConnection extends LeaseConnection {
    private final UnicastProcessor<Frame> setupFrames = UnicastProcessor.create();
    private final boolean serverLeaseEnabled;
    private volatile Disposable leaseErrorSubs = () -> {};

    public ServerLeaseSetupConnection(
        DuplexConnection setupConnection, Context context, boolean serverLeaseEnabled) {
      super(setupConnection, context,"server");
      this.serverLeaseEnabled = serverLeaseEnabled;
    }

    @Override
    public Flux<Frame> receive() {
      super.receive()
          .next()
          .subscribe(
              f -> {
                boolean clientLeaseEnabled = isSetup(f) && Frame.Setup.supportsLease(f);
                if (clientLeaseEnabled && !serverLeaseEnabled) {
                  UnsupportedSetupException error =
                      new UnsupportedSetupException("Server does not support lease");
                  leaseErrorSubs = sendOne(Frame.Error.from(0, error)).then(close()).subscribe();
                }
                if (clientLeaseEnabled) {
                  enableLease();
                }
                setupFrames.onNext(f);
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
    private static final Logger LOGGER = LoggerFactory.getLogger("io.rsocket.lease.RequestConnection");
    public RequestConnection(String classifier,DuplexConnection source, Context context, LeaseManager leaseManager) {
      super(source, context,classifier);
      this.leaseManager = leaseManager;
      LOGGER.info(String.format("Added %s for %s",getClass().getName(),getClassifier()));
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
          throw new NoLeaseException(lease,getClassifier());
        }
      } else {
        return f;
      }
    }
  }

  static class RequestOutboundConnection extends RequestConnection {

    public RequestOutboundConnection(
        String classifier,
        DuplexConnection sourceConnection,
        Context context,
        LeaseManager requesterLeaseManager) {
      super(classifier,sourceConnection, context, requesterLeaseManager);
    }

    @Override
    public Mono<Void> send(Publisher<Frame> frames) {
        return Flux.from(frames).concatMap(frame -> {
          Function<Frame, Frame> f = isLeaseEnabled()
                  ? this::handleFrame
                  : Function.identity();
            return mono(f).apply(frame);
        }).then();
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
        String classifier, DuplexConnection peerConnection, Context context, LeaseManager responderLeaseManager) {
      super(classifier,peerConnection, context, responderLeaseManager);
    }

    @Override
    public Flux<Frame> receive() {
        return super.receive().map(frame -> isLeaseEnabled()
                ? handleFrame(frame)
                : frame);
    }
  }

  static class LeaseInConnection extends LeaseConnection {

    private final Consumer<Lease> leaseConsumer;

    public LeaseInConnection(
        String classifier, DuplexConnection source, Context context, Consumer<Lease> leaseConsumer) {
      super(source, context,classifier);
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

    public Args(
        LeaseManager requesterLeaseManager,
        LeaseManager responderLeaseManager,
        Consumer<Lease> leaseConsumer) {
      this.requesterLeaseManager = requesterLeaseManager;
      this.responderLeaseManager = responderLeaseManager;
      this.leaseConsumer = leaseConsumer;
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
  }

  private static boolean isRequest(Frame frame) {
    return requests.contains(frame.getType());
  }

  private static boolean isSetup(Frame frame) {
    return frame.getType().equals(SETUP);
  }

  private static final Set<FrameType> requests =
      new HashSet<>(
          Arrays.asList(REQUEST_CHANNEL, REQUEST_RESPONSE, REQUEST_STREAM, FIRE_AND_FORGET));
}
