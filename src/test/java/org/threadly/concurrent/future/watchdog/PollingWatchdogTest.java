package org.threadly.concurrent.future.watchdog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.NoThreadScheduler;
import org.threadly.concurrent.future.FutureUtils;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.SettableListenableFuture;

@SuppressWarnings("javadoc")
public class PollingWatchdogTest {
  private NoThreadScheduler scheduler;
  private PollingWatchdog watchdog;
  private AtomicBoolean cancelFuture;
  
  @Before
  public void setup() {
    scheduler = new NoThreadScheduler();
    watchdog = new PollingWatchdog(scheduler, 1, true);
    cancelFuture = new AtomicBoolean(false);
  }
  
  @After
  public void cleanup() {
    scheduler = null;
    watchdog = null;
    cancelFuture = null;
  }
  
  @Test
  public void isActiveTest() throws InterruptedException {
    assertFalse(watchdog.isActive());
    
    ListenableFuture<?> future = FutureUtils.immediateResultFuture(null);
    watchdog.watch(cancelFuture::get, future);
    
    assertFalse(watchdog.isActive());
    
    SettableListenableFuture<?> slf = new SettableListenableFuture<>();
    watchdog.watch(cancelFuture::get, slf);

    assertTrue(watchdog.isActive());
    
    cancelFuture.set(true);
    assertEquals(1, scheduler.blockingTick(null));
    
    assertFalse(watchdog.isActive());
  }
  
  @Test
  public void alreadyDoneFutureWatchTest() {
    ListenableFuture<?> future = FutureUtils.immediateResultFuture(null);
    watchdog.watch(cancelFuture::get, future);

    assertEquals(0, watchdog.getWatchingCount());
  }
  
  @Test
  public void futureFinishTest() {
    SettableListenableFuture<?> slf = new SettableListenableFuture<>();
    
    watchdog.watch(cancelFuture::get, slf);

    assertEquals(1, watchdog.getWatchingCount());
    
    slf.setResult(null);

    assertEquals(0, watchdog.getWatchingCount());
  }
  
  @Test
  public void watchdogCancelFutureTest() throws InterruptedException {
    SettableListenableFuture<?> slf = new SettableListenableFuture<>();
    watchdog.watch(cancelFuture::get, slf);
    
    cancelFuture.set(true);
    assertEquals(1, scheduler.blockingTick(null));
    
    assertTrue(slf.isCancelled());
    assertEquals(0, watchdog.getWatchingCount());
  }
}
