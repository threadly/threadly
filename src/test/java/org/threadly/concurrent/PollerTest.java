package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.future.FutureUtils;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.SettableListenableFuture;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.test.concurrent.TestableScheduler;
import org.threadly.util.StringUtils;

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
    poller = new Poller(CentralThreadlyPool.computationPool(), POLL_INTERVAL, 10);
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
  
  @Test
  public void consumeQueueTest() throws InterruptedException, TimeoutException {
    poller = new Poller(CentralThreadlyPool.computationPool(), 10);
    Queue<String> itemQueue = new LinkedList<>();
    for (int i = 0; i < TEST_QTY; i++) {
      itemQueue.add(StringUtils.makeRandomString(5));
    }
    Iterator<String> verificationIt = new LinkedList<>(itemQueue).iterator();
    AsyncVerifier av = new AsyncVerifier();
    
    ListenableFuture<?> lf = poller.consumeQueue(itemQueue, (s) -> {
      av.assertEquals(verificationIt.next(), s);
      av.signalComplete();
    });

    av.waitForTest(1_000, TEST_QTY);
    assertFalse(lf.isDone());
    assertTrue(itemQueue.isEmpty());
    assertTrue(lf.cancel(true));
  }
  
  @Test
  public void consumeLargeQueueTest() throws InterruptedException, TimeoutException {
    int count = 2_000_000;
    poller = new Poller(CentralThreadlyPool.computationPool(), 10);
    Queue<String> itemQueue = new LinkedList<>();
    for (int i = 0; i < count; i++) {
      itemQueue.add(StringUtils.makeRandomString(5));
    }
    AsyncVerifier av = new AsyncVerifier();
    
    ListenableFuture<?> lf = poller.consumeQueue(itemQueue, (s) -> {
      av.signalComplete();
    });

    av.waitForTest(10_000, count);
    assertFalse(lf.isDone());
    assertTrue(itemQueue.isEmpty());
    assertTrue(lf.cancel(true));
  }
  
  @Test
  public void consumeQueueStopTest() throws InterruptedException, TimeoutException {
    poller = new Poller(CentralThreadlyPool.computationPool(), 10);
    Queue<String> itemQueue = new LinkedList<>();
    AsyncVerifier av = new AsyncVerifier();
    
    ListenableFuture<?> lf = poller.consumeQueue(itemQueue, (s) -> {
      av.signalComplete();
    });
    
    for (int i = 1; i <= TEST_QTY; i++) {
      itemQueue.add(StringUtils.makeRandomString(4));
      av.waitForTest(200, i);
      assertTrue(itemQueue.isEmpty());
    }
    
    assertTrue(lf.cancel(true));

    for (int i = 1; i <= TEST_QTY; i++) {
      itemQueue.add(StringUtils.makeRandomString(4));
    }
    TestUtils.sleep(100);
    
    assertEquals(TEST_QTY, itemQueue.size()); // nothing should have been consumed
  }
  
  @Test
  public void consumeQueueHandleErrorTest() throws InterruptedException, TimeoutException {
    poller = new Poller(CentralThreadlyPool.computationPool(), 10);
    RuntimeException error = new RuntimeException();
    Queue<String> itemQueue = new LinkedList<>();
    String first = null;
    for (int i = 0; i < TEST_QTY; i++) {
      String s = StringUtils.makeRandomString(5);
      if (first == null) {
        first = s;
      }
      itemQueue.add(s);
    }
    AsyncVerifier av = new AsyncVerifier();
    TestRunnable errorRun = new TestRunnable();
    
    final String fFirst = first;
    ListenableFuture<?> lf = poller.consumeQueue(itemQueue, (s) -> {
      av.signalComplete();
      if (s == fFirst) {
        throw error;
      }
    }, (e) -> errorRun.run());

    av.waitForTest(1_000, TEST_QTY);
    assertEquals(1, errorRun.getRunCount());
    
    assertFalse(lf.isDone());
    assertTrue(lf.cancel(true));
  }
  
  @Test
  public void consumeQueueUnhandleErrorTest() throws InterruptedException, TimeoutException {
    poller = new Poller(CentralThreadlyPool.computationPool(), 10);
    RuntimeException error = new RuntimeException();
    Queue<String> itemQueue = new LinkedList<>();
    String last = null;
    for (int i = 0; i < TEST_QTY; i++) {
      last = StringUtils.makeRandomString(5);
      itemQueue.add(last);
    }
    
    final String fLast = last;
    ListenableFuture<?> lf = poller.consumeQueue(itemQueue, (s) -> {
      if (s == fLast) {
        throw error;
      }
    }, null);
    
    try {
      lf.get(1_000, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == error);
    }
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
