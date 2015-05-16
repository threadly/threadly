package org.threadly.concurrent.future;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.NoThreadScheduler;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class WatchdogTest {
  private static final int TIMEOUT = 1;
  
  private NoThreadScheduler scheduler;
  private Watchdog watchdog;
  
  @Before
  public void setup() {
    scheduler = new NoThreadScheduler();
    watchdog = new Watchdog(scheduler, TIMEOUT, true);
  }
  
  @After
  public void cleanup() {
    scheduler = null;
    watchdog = null;
  }
  
  @Test
  public void emptySchedulerConstructorTest() {
    watchdog = new Watchdog(1, true);
    
    assertNotNull(watchdog.scheduler);
  }
  
  @Test
  public void getTimeoutInMillisTest() {
    assertEquals(TIMEOUT, watchdog.getTimeoutInMillis());
  }
  
  @Test
  public void isActiveTest() {
    assertFalse(watchdog.isActive());
    
    ListenableFuture<Object> future = FutureUtils.immediateResultFuture(null);
    watchdog.watch(future);
    
    assertFalse(watchdog.isActive());
    
    SettableListenableFuture<Object> slf = new SettableListenableFuture<Object>();
    watchdog.watch(slf);

    assertTrue(watchdog.isActive());
    
    TestUtils.blockTillClockAdvances();
    assertEquals(1, scheduler.tick(null));
    
    assertFalse(watchdog.isActive());
  }
  
  @Test
  public void alreadyDoneFutureWatchTest() {
    ListenableFuture<Object> future = FutureUtils.immediateResultFuture(null);
    watchdog.watch(future);
    
    assertTrue(watchdog.futures.isEmpty());
  }
  
  @Test
  public void futureFinishTest() {
    SettableListenableFuture<Object> slf = new SettableListenableFuture<Object>();
    
    watchdog.watch(slf);
    
    assertEquals(1, watchdog.futures.size());
    
    slf.setResult(null);
    
    assertTrue(watchdog.futures.isEmpty());
  }
  
  @Test
  public void expiredFutureTest() {
    SettableListenableFuture<Object> slf = new SettableListenableFuture<Object>();
    watchdog.watch(slf);
    
    TestUtils.blockTillClockAdvances();
    
    assertEquals(1, scheduler.tick(null));
    
    assertTrue(slf.isCancelled());
    assertTrue(watchdog.futures.isEmpty());
  }
  
  @Test
  public void rescheduledFutureCheckTest() throws InterruptedException {
    watchdog = new Watchdog(scheduler, DELAY_TIME * 2, true);
    SettableListenableFuture<Object> slf1 = new SettableListenableFuture<Object>();
    watchdog.watch(slf1);
    TestUtils.sleep(DELAY_TIME);
    SettableListenableFuture<Object> slf2 = new SettableListenableFuture<Object>();
    watchdog.watch(slf2);
    
    assertEquals(1, scheduler.blockingTick(null));
    assertTrue(slf1.isCancelled());
    assertFalse(slf2.isCancelled());
    
    assertEquals(1, scheduler.blockingTick(null));
    assertTrue(slf1.isCancelled());
    assertTrue(slf2.isCancelled());
  }
}
