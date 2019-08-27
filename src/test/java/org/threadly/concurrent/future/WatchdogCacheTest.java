package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
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
  @SuppressWarnings("deprecation")
  public void booleanSchedulerConstructorTest() {
    watchdog = new WatchdogCache(true);
    
    assertNotNull(watchdog.scheduler);
  }
  
  @Test
  public void centralWatchdogCacheConstructorTest() {
    watchdog = WatchdogCache.centralWatchdogCache(true);
    
    assertNotNull(watchdog.scheduler);
  }
  
  @Test
  public void alreadyDoneFutureWatchTest() {
    ListenableFuture<Object> future = FutureUtils.immediateResultFuture(null);
    watchdog.watch(TIMEOUT, future);
    
    assertTrue(watchdog.cachedDogs.isEmpty());
  }
  
  @Test
  public void expiredFutureTest() {
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    watchdog.watch(TIMEOUT, slf);
    
    TestUtils.blockTillClockAdvances();
    
    assertEquals(1, scheduler.tick());
    
    assertTrue(slf.isCancelled());
  }
  
  @Test
  public void cacheCleanTest() {
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    watchdog.watch(TIMEOUT, slf);
    assertFalse(watchdog.cachedDogs.isEmpty());
    
    TestUtils.blockTillClockAdvances();
    
    assertEquals(2, scheduler.advance(WatchdogCache.INSPECTION_INTERVAL_MILLIS));
    
    assertTrue(watchdog.cachedDogs.isEmpty());
  }
  
  @Test
  public void resolutionTest() {
    watchdog = new WatchdogCache(scheduler, true);
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    watchdog.watch(WatchdogCache.DEFAULT_RESOLUTION_MILLIS, slf);
    watchdog.watch(WatchdogCache.DEFAULT_RESOLUTION_MILLIS / 2, slf);
    watchdog.watch(WatchdogCache.DEFAULT_RESOLUTION_MILLIS / 4, slf);
    
    assertEquals(1, watchdog.cachedDogs.size());
    
    watchdog.watch(WatchdogCache.DEFAULT_RESOLUTION_MILLIS + 1, slf);
    
    assertEquals(2, watchdog.cachedDogs.size());
  }
}
