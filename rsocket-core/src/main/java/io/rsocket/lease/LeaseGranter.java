package io.rsocket.lease;

import java.nio.ByteBuffer;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

public interface LeaseGranter {

  Mono<Void> grantLease(int requests, int ttl, @Nullable ByteBuffer metadata);
}
