package org.threadly.concurrent.future.watchdog;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.NoThreadScheduler;
import org.threadly.concurrent.future.FutureUtils;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.SettableListenableFuture;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class ConstantTimeWatchdogTest extends ThreadlyTester {
  private static final int TIMEOUT = 2;
  
  private NoThreadScheduler scheduler;
  private ConstantTimeWatchdog watchdog;
  
  @BeforeEach
  public void setup() {
    scheduler = new NoThreadScheduler();
    watchdog = new ConstantTimeWatchdog(scheduler, TIMEOUT, true);
  }
  
  @AfterEach
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
  public void getTimeoutInMillisTest() {
    assertEquals(TIMEOUT, watchdog.getTimeoutInMillis());
  }
  
  @Test
  public void isActiveTest() {
    assertFalse(watchdog.isActive());
    
    ListenableFuture<?> future = FutureUtils.immediateResultFuture(null);
    watchdog.watch(future);
    
    assertFalse(watchdog.isActive());
    
    SettableListenableFuture<?> slf = new SettableListenableFuture<>();
    watchdog.watch(slf);

    assertTrue(watchdog.isActive());
    
    waitForTimeout();
    assertEquals(1, scheduler.tick(null));
    
    assertFalse(watchdog.isActive());
  }
  
  @Test
  public void alreadyDoneFutureWatchTest() {
    ListenableFuture<?> future = FutureUtils.immediateResultFuture(null);
    watchdog.watch(future);

    assertEquals(0, watchdog.getWatchingCount());
  }
  
  @Test
  public void futureFinishTest() {
    SettableListenableFuture<?> slf = new SettableListenableFuture<>();
    
    watchdog.watch(slf);

    assertEquals(1, watchdog.getWatchingCount());
    
    slf.setResult(null);

    assertEquals(0, watchdog.getWatchingCount());
  }
  
  @Test
  public void expiredFutureTest() {
    SettableListenableFuture<?> slf = new SettableListenableFuture<>();
    watchdog.watch(slf);

    waitForTimeout();
    
    assertEquals(1, scheduler.tick(null));
    
    assertTrue(slf.isCancelled());
    assertEquals(0, watchdog.getWatchingCount());
  }
  
  @Test
  public void rescheduledFutureCheckTest() throws InterruptedException {
    long delayTime = 100; // longer than constants DELAY_TIME to ensure we can tick BEFORE the second future times out
    watchdog = new ConstantTimeWatchdog(scheduler, delayTime * 2, true);
    SettableListenableFuture<?> slf1 = new SettableListenableFuture<>();
    watchdog.watch(slf1);
    TestUtils.sleep(delayTime);
    SettableListenableFuture<?> slf2 = new SettableListenableFuture<>();
    watchdog.watch(slf2);
    
    assertEquals(1, scheduler.blockingTick(null));
    assertTrue(slf1.isCancelled());
    assertFalse(slf2.isCancelled());
    
    assertEquals(1, scheduler.blockingTick(null));
    assertTrue(slf1.isCancelled());
    assertTrue(slf2.isCancelled());
  }
}
