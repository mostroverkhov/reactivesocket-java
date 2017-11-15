package io.rsocket.examples.transport.tcp.lease;

import static java.time.Duration.*;

import io.rsocket.*;
import io.rsocket.lease.LeaseConnectionRef;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import java.util.Date;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class LeaseClientServeReqRep {
  private static final Logger LOGGER = LoggerFactory.getLogger("io.rsocket.examples.lease_req_rep");

  public static void main(String[] args) {

    LeaseControlSource serverLeaseControl = new LeaseControlSource();
    NettyContextCloseable nettyContextCloseable =
        CoolRSocketFactory.receive()
            .enableLease(serverLeaseControl)
            .acceptor(
                (setup, reactiveSocket) ->
                    Mono.just(
                        new AbstractRSocket() {
                          @Override
                          public Mono<Payload> requestResponse(Payload payload) {
                            return Mono.just(new PayloadImpl("Server Response " + new Date()));
                          }
                        }))
            .transport(TcpServerTransport.create("localhost", 7000))
            .start()
            .block();

    LeaseControlSource clientLeaseControl = new LeaseControlSource();
    RSocket clientSocket =
        CoolRSocketFactory.connect()
            .enableLease(clientLeaseControl)
            .keepAlive(Duration.ofSeconds(1), 3, keeps -> {})
            .transport(TcpClientTransport.create("localhost", 7000))
            .start()
            .block();

    clientLeaseControl
        .leaseConnection()
        .flatMapMany(LeaseConnectionRef::inboundLease)
        .subscribe(
            s ->
                LOGGER.info(
                    String.format(
                        "Client received lease: Reqs: %d TTL: %d",
                        s.getAllowedRequests(), s.getTtl())));

    Flux.interval(ofSeconds(1))
        .flatMap(
            signal -> {
              LOGGER.info("Availability: " + clientSocket.availability());
              return clientSocket
                  .requestResponse(new PayloadImpl("Client request " + new Date()))
                  .onErrorResume(
                      err ->
                          Mono.<Payload>empty().doOnTerminate(() -> LOGGER.info("Error: " + err)));
            })
        .subscribe(resp -> LOGGER.info("Client response: " + resp.getDataUtf8()));

    serverLeaseControl
        .leaseConnection()
        .flatMapMany(connRef -> Flux.interval(ofSeconds(1), ofSeconds(10)).map(signal -> connRef))
        .subscribe(ref -> ref.grantLease(7, 5_000));

    clientSocket.onClose().block();
  }

  private static class LeaseControlSource implements Consumer<LeaseConnectionRef> {
    private final MonoProcessor<LeaseConnectionRef> leaseControlMono = MonoProcessor.create();

    public Mono<LeaseConnectionRef> leaseConnection() {
      return leaseControlMono;
    }

    @Override
    public void accept(LeaseConnectionRef leaseControl) {
      leaseControlMono.onNext(leaseControl);
    }
  }
}
