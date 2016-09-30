package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class WatchdogCacheTest {
  private static final int TIMEOUT = 1;
  
  private TestableScheduler scheduler;
  private WatchdogCache watchdog;
  
  @Before
  public void setup() {
    scheduler = new TestableScheduler();
    watchdog = new WatchdogCache(scheduler, true);
  }
  
  @After
  public void cleanup() {
    scheduler = null;
    watchdog = null;
  }
  
  @Test
  public void emptySchedulerConstructorTest() {
    watchdog = new WatchdogCache(true);
    
    assertNotNull(watchdog.scheduler);
  }
  
  @Test
  public void alreadyDoneFutureWatchTest() {
    ListenableFuture<Object> future = FutureUtils.immediateResultFuture(null);
    watchdog.watch(future, TIMEOUT);
    
    assertTrue(watchdog.cachedDogs.isEmpty());
  }
  
  @Test
  public void expiredFutureTest() {
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    watchdog.watch(slf, TIMEOUT);
    
    TestUtils.blockTillClockAdvances();
    
    assertEquals(1, scheduler.tick());
    
    assertTrue(slf.isCancelled());
  }
  
  @Test
  public void cacheCleanTest() {
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    watchdog.watch(slf, TIMEOUT);
    assertFalse(watchdog.cachedDogs.isEmpty());
    
    TestUtils.blockTillClockAdvances();
    
    assertEquals(2, scheduler.advance(WatchdogCache.INSPECTION_INTERVAL_MILLIS));
    
    assertTrue(watchdog.cachedDogs.isEmpty());
  }
}
