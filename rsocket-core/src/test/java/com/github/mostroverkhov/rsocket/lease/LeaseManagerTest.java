package com.github.mostroverkhov.rsocket.lease;

import static org.junit.Assert.assertEquals;

import com.github.mostroverkhov.rsocket.exceptions.NoLeaseException;
import java.time.Duration;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;

public class LeaseManagerTest {

  private LeaseManager leaseManager;

  @Before
  public void setUp() throws Exception {
    leaseManager = new LeaseManager("");
  }

  @Test
  public void initialLeaseAvailability() throws Exception {
    assertEquals(0.0, leaseManager.availability(), 1e-5);
  }

  @Test(expected = NoLeaseException.class)
  public void useNoRequests() throws Exception {
    leaseManager.useLease();
  }

  @Test
  public void grant() throws Exception {
    leaseManager.grantLease(2, 100);
    assertEquals(1.0, leaseManager.availability(), 1e-5);
  }

  @Test(expected = IllegalArgumentException.class)
  public void grantLeaseZeroRequests() throws Exception {
    leaseManager.grantLease(0, 100);
  }

  @Test(expected = IllegalArgumentException.class)
  public void grantLeaseZeroTtl() throws Exception {
    leaseManager.grantLease(1, 0);
  }

  @Test
  public void use() throws Exception {
    leaseManager.grantLease(2, 100);
    leaseManager.useLease();
    assertEquals(0.5, leaseManager.availability(), 1e-5);
  }

  @Test(expected = NoLeaseException.class)
  public void useTimeout() throws Exception {
    leaseManager.grantLease(2, 1);
    Mono.delay(Duration.ofMillis(1500)).block();
    leaseManager.useLease();
  }
}
