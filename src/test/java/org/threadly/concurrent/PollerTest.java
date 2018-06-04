package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.SingleThreadScheduler;
import org.threadly.concurrent.future.FutureUtils;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.SettableListenableFuture;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class PollerTest extends ThreadlyTester {
  private static final int POLL_INTERVAL = 10_000;

  private TestableScheduler scheduler;
  private Poller poller;

  @Before
  public void setup() {
    scheduler = new TestableScheduler();
    poller = new Poller(scheduler, POLL_INTERVAL);
  }

  @After
  public void cleanup() {
    scheduler = null;
    poller = null;
  }

  @Test
  public void simplePollTest() {
    AtomicBoolean pollState = new AtomicBoolean(false);

    ListenableFuture<?> f = poller.watch(() -> pollState.get());
    assertFalse(f.isDone());

    assertEquals(1, scheduler.advance(POLL_INTERVAL));
    assertFalse(f.isDone());

    pollState.set(true);

    assertEquals(1, scheduler.advance(POLL_INTERVAL));
    assertTrue(f.isDone());

    assertEquals(0, scheduler.advance(POLL_INTERVAL));
  }

  @Test
  public void timeoutTest() {
    poller = new Poller(new SingleThreadScheduler(), POLL_INTERVAL, 10);
    ListenableFuture<?> f = poller.watch(() -> false);
    TestUtils.sleep(10);

    new TestCondition(() -> f.isDone() && f.isCancelled())
        .blockTillTrue();// will throw if does not become true
  }
  
  @Test
  public void watchListenableFutureWithExtendedResultTest() {
    ListenableFuture<DoNothingRunnable> lf = FutureUtils.immediateResultFuture(DoNothingRunnable.instance());
    ListenableFuture<Runnable> lfResult = poller.watch(lf);
    
    assertTrue(((ListenableFuture<?>)lfResult) == lf);
  }
  
  @Test
  public void watchAlreadyDoneFutureWithResultTest() throws InterruptedException, ExecutionException {
    final Object objResult = new Object();
    ListenableFuture<Object> lfResult = poller.watch(new AlreadyDoneFuture() {
      @Override
      public Object get() throws ExecutionException {
        return objResult;
      }

      @Override
      public Object get(long timeout, TimeUnit unit) throws ExecutionException {
        return objResult;
      }
    });
    
    assertTrue(lfResult.isDone());
    assertTrue(objResult == lfResult.get());
  }
  
  @Test
  public void watchAlreadyDoneFutureWithFailureTest() throws InterruptedException {
    final Throwable rootCause = new Exception();
    ListenableFuture<Object> lfResult = poller.watch(new AlreadyDoneFuture() {
      @Override
      public Object get() throws ExecutionException {
        throw new ExecutionException(rootCause);
      }

      @Override
      public Object get(long timeout, TimeUnit unit) throws ExecutionException {
        throw new ExecutionException(rootCause);
      }
    });
    
    assertTrue(lfResult.isDone());
    try {
      lfResult.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == rootCause);
    }
  }
  
  @Test
  public void watchAlreadyCanceledFutureTest() {
    Future<Object> canceledFuture = new FutureTask<>(() -> { return null; });
    assertTrue(canceledFuture.cancel(true));
    ListenableFuture<Object> lfResult = poller.watch(canceledFuture);
    
    assertTrue(lfResult.isDone());
    assertTrue(lfResult.isCancelled());
  }
  
  @Test
  public void watchIncompleteFutureWithResultTest() throws InterruptedException, ExecutionException {
    poller = new Poller(scheduler, POLL_INTERVAL, POLL_INTERVAL * 2);  // must have timeout to avoid just casting SLF
    Object objResult = new Object();
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    ListenableFuture<Object> lfResult = poller.watch(slf);
    
    assertFalse(lfResult.isDone());
    
    slf.setResult(objResult);
    assertEquals(1, scheduler.advance(POLL_INTERVAL));

    assertTrue(lfResult.isDone());
    assertTrue(objResult == lfResult.get());
  }
  
  @Test
  public void watchIncompleteFutureWithFailureTest() throws InterruptedException {
    poller = new Poller(scheduler, POLL_INTERVAL, POLL_INTERVAL * 2);  // must have timeout to avoid just casting SLF
    Throwable rootCause = new Exception();
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    ListenableFuture<Object> lfResult = poller.watch(slf);
    
    assertFalse(lfResult.isDone());
    
    slf.setFailure(rootCause);
    assertEquals(1, scheduler.advance(POLL_INTERVAL));
    
    assertTrue(lfResult.isDone());
    try {
      lfResult.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == rootCause);
    }
  }
  
  @Test
  public void watchIncompleteCanceledFutureTest() {
    poller = new Poller(scheduler, POLL_INTERVAL, POLL_INTERVAL * 2);  // must have timeout to avoid just casting SLF
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    ListenableFuture<Object> lfResult = poller.watch(slf);
    
    assertFalse(lfResult.isDone());
    
    slf.cancel(false);
    assertEquals(1, scheduler.advance(POLL_INTERVAL));
    
    assertTrue(lfResult.isDone());
    assertTrue(lfResult.isCancelled());
  }
  
  private static abstract class AlreadyDoneFuture implements Future<Object> {
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }
  }
}
