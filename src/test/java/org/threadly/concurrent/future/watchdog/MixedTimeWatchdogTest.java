package org.threadly.concurrent.future.watchdog;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.future.FutureUtils;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.SettableListenableFuture;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.test.concurrent.TestableScheduler;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class MixedTimeWatchdogTest extends ThreadlyTester {
  private static final int TIMEOUT = 2;
  
  private TestableScheduler scheduler;
  private MixedTimeWatchdog watchdog;
  
  @Before
  public void setup() {
    scheduler = new TestableScheduler();
    watchdog = new MixedTimeWatchdog(scheduler, true, 1);
  }
  
  @After
  public void cleanup() {
    scheduler = null;
    watchdog = null;
  }
  
  @SuppressWarnings("unused")
  private static void waitForTimeout() {
    if (TIMEOUT > 1) {
      TestUtils.sleep(TIMEOUT);
      Clock.accurateTimeNanos();
    } else {
      TestUtils.blockTillClockAdvances();
    }
  }
  
  @Test
  public void centralWatchdogCacheConstructorTest() {
    watchdog = MixedTimeWatchdog.centralWatchdog(true);
    
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

    waitForTimeout();
    
    assertEquals(1, scheduler.tick());
    
    assertTrue(slf.isCancelled());
  }
  
  @Test
  public void cacheCleanTest() {
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    watchdog.watch(TIMEOUT, slf);
    assertFalse(watchdog.cachedDogs.isEmpty());

    waitForTimeout();
    
    assertEquals(2, scheduler.advance(MixedTimeWatchdog.INSPECTION_INTERVAL_MILLIS));
    
    assertTrue(watchdog.cachedDogs.isEmpty());
  }
  
  @Test
  public void resolutionTest() {
    watchdog = new MixedTimeWatchdog(scheduler, true);
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    watchdog.watch(MixedTimeWatchdog.DEFAULT_RESOLUTION_MILLIS, slf);
    watchdog.watch(MixedTimeWatchdog.DEFAULT_RESOLUTION_MILLIS / 2, slf);
    watchdog.watch(MixedTimeWatchdog.DEFAULT_RESOLUTION_MILLIS / 4, slf);
    
    assertEquals(1, watchdog.cachedDogs.size());
    
    watchdog.watch(MixedTimeWatchdog.DEFAULT_RESOLUTION_MILLIS + 1, slf);
    
    assertEquals(2, watchdog.cachedDogs.size());
  }
}
