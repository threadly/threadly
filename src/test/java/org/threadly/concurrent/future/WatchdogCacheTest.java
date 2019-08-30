package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings({"javadoc", "deprecation"})
public class WatchdogCacheTest extends ThreadlyTester {
  private static final int TIMEOUT = 1;
  
  private TestableScheduler scheduler;
  private WatchdogCache watchdog;
  
  @Before
  public void setup() {
    scheduler = new TestableScheduler();
    watchdog = new WatchdogCache(scheduler, true, 1);
  }
  
  @After
  public void cleanup() {
    scheduler = null;
    watchdog = null;
  }
  
  @Test
  public void alreadyDoneFutureWatchTest() {
    ListenableFuture<Object> future = FutureUtils.immediateResultFuture(null);
    watchdog.watch(TIMEOUT, future);

    assertEquals(0, watchdog.getWatchingCount());
  }
  
  @Test
  public void expiredFutureTest() {
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    watchdog.watch(TIMEOUT, slf);
    
    TestUtils.blockTillClockAdvances();
    
    assertEquals(1, scheduler.tick());
    
    assertTrue(slf.isCancelled());
  }
}
