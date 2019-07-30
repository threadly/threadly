package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.SingleThreadScheduler;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;
import org.threadly.util.StringUtils;
import org.threadly.util.StackSuppressedRuntimeException;

@SuppressWarnings("javadoc")
public class FutureUtilsTest extends ThreadlyTester {
  private static <T> List<ListenableFuture<? extends T>> makeFutures(int count, int errorIndex) {
    List<ListenableFuture<? extends T>> result = new ArrayList<>(count + 1);
    
    for (int i = 0; i < count; i++) {
      if (i == errorIndex) {
        result.add(FutureUtils.<T>immediateFailureFuture(null));
      } else {
        result.add(FutureUtils.<T>immediateResultFuture(null));
      }
    }
    
    return result;
  }
  
  @Test
  public void blockTillAllCompleteNullTest() throws InterruptedException {
    FutureUtils.blockTillAllComplete((Iterable<Future<?>>)null); // should return immediately
  }
  
  @Test
  public void blockTillAllCompleteTest() throws InterruptedException {
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, -1);
    
    FutureUtils.blockTillAllComplete(futures);
    
    Iterator<ListenableFuture<?>> it = futures.iterator();
    while (it.hasNext()) {
      assertTrue(it.next().isDone());
    }
  }
  
  @Test
  public void blockTillAllCompleteErrorTest() throws InterruptedException {
    int errorIndex = TEST_QTY / 2;
    
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, errorIndex);
    
    FutureUtils.blockTillAllComplete(futures);
    
    Iterator<ListenableFuture<?>> it = futures.iterator();
    while (it.hasNext()) {
      assertTrue(it.next().isDone());
    }
  }
  
  @Test
  public void blockTillAllCompleteWithTimeoutNullTest() throws InterruptedException, TimeoutException {
    FutureUtils.blockTillAllComplete(null, 1); // should return immediately
  }
  
  @Test
  public void blockTillAllCompleteWithTimeoutTest() throws InterruptedException, TimeoutException {
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, -1);
    
    FutureUtils.blockTillAllComplete(futures, 1000 * 10);
    
    Iterator<ListenableFuture<?>> it = futures.iterator();
    while (it.hasNext()) {
      assertTrue(it.next().isDone());
    }
  }
  
  @Test
  public void blockTillAllCompleteWithTimeoutErrorTest() throws InterruptedException, TimeoutException {
    int errorIndex = TEST_QTY / 2;
    
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, errorIndex);
    
    FutureUtils.blockTillAllComplete(futures, 1000 * 10);
    
    Iterator<ListenableFuture<?>> it = futures.iterator();
    while (it.hasNext()) {
      assertTrue(it.next().isDone());
    }
  }
  
  @Test (expected = TimeoutException.class)
  public void blockTillAllCompleteWithTimeoutTimeoutTest() throws InterruptedException, TimeoutException {
    List<? extends ListenableFuture<?>> futures = Collections.singletonList(new SettableListenableFuture<>());
    
    FutureUtils.blockTillAllComplete(futures, 100);
    fail("Exception should have thrown");
  }
  
  @Test (expected = TimeoutException.class)
  public void blockTillAllCompleteWithTimeoutZeroTimeoutTest() throws InterruptedException, TimeoutException {
    List<? extends ListenableFuture<?>> futures = Collections.singletonList(new SettableListenableFuture<>());
    
    FutureUtils.blockTillAllComplete(futures, 0);
    fail("Exception should have thrown");
  }
  
  @Test
  public void blockTillAllCompleteOrFirstErrorNullTest() throws InterruptedException, ExecutionException {
    FutureUtils.blockTillAllCompleteOrFirstError((Iterable<Future<?>>)null); // should return immediately
  }
  
  @Test
  public void blockTillAllCompleteOrFirstErrorTest() throws InterruptedException, ExecutionException {
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, -1);
    
    FutureUtils.blockTillAllCompleteOrFirstError(futures);
    
    Iterator<ListenableFuture<?>> it = futures.iterator();
    while (it.hasNext()) {
      assertTrue(it.next().isDone());
    }
  }
  
  @Test
  public void blockTillAllCompleteOrFirstErrorErrorTest() throws InterruptedException {
    int errorIndex = TEST_QTY / 2;
    
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, errorIndex);
    
    FutureUtils.blockTillAllComplete(futures);

    Iterator<ListenableFuture<?>> it = futures.iterator();
    for (int i = 0; i <= errorIndex; i++) {
      Future<?> f = it.next();
      
      if (i < errorIndex) {
        assertTrue(f.isDone());
      } else if (i == errorIndex) {
        try {
          f.get();
          fail("Exception should have thrown");
        } catch (ExecutionException e) {
          // expected
        }
      }
    }
  }
  
  @Test
  public void blockTillAllCompleteOrFirstErrorWithTimeoutNullTest() throws InterruptedException, ExecutionException, TimeoutException {
    FutureUtils.blockTillAllCompleteOrFirstError(null, 10); // should return immediately
  }
  
  @Test
  public void blockTillAllCompleteOrFirstErrorWithTimeoutTest() throws InterruptedException, ExecutionException, TimeoutException {
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, -1);
    
    FutureUtils.blockTillAllCompleteOrFirstError(futures, 1000 * 10);
    
    Iterator<ListenableFuture<?>> it = futures.iterator();
    while (it.hasNext()) {
      assertTrue(it.next().isDone());
    }
  }
  
  @Test
  public void blockTillAllCompleteOrFirstErrorWithTimeoutErrorTest() throws InterruptedException, TimeoutException {
    int errorIndex = TEST_QTY / 2;
    
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, errorIndex);
    
    FutureUtils.blockTillAllComplete(futures, 1000 * 10);

    Iterator<ListenableFuture<?>> it = futures.iterator();
    for (int i = 0; i <= errorIndex; i++) {
      Future<?> f = it.next();
      
      if (i < errorIndex) {
        assertTrue(f.isDone());
      } else if (i == errorIndex) {
        try {
          f.get();
          fail("Exception should have thrown");
        } catch (ExecutionException e) {
          // expected
        }
      }
    }
  }
  
  @Test (expected = TimeoutException.class)
  public void blockTillAllCompleteOrFirstErrorWithTimeoutTimeoutTest() throws InterruptedException, 
                                                                              TimeoutException, ExecutionException {
    List<? extends ListenableFuture<?>> futures = Collections.singletonList(new SettableListenableFuture<>());
    
    FutureUtils.blockTillAllCompleteOrFirstError(futures, 100);
    fail("Exception should have thrown");
  }
  
  @Test (expected = TimeoutException.class)
  public void blockTillAllCompleteOrFirstErrorWithTimeoutZeroTimeoutTest() throws InterruptedException, 
                                                                                  TimeoutException, ExecutionException {
    List<? extends ListenableFuture<?>> futures = Collections.singletonList(new SettableListenableFuture<>());
    
    FutureUtils.blockTillAllCompleteOrFirstError(futures, 0);
    fail("Exception should have thrown");
  }
  
  @Test
  public void countFuturesWithResultTest() throws InterruptedException {
    List<ListenableFuture<Boolean>> futures = new ArrayList<>(TEST_QTY * 2);
    for (int i = 0; i < TEST_QTY * 2; i++) {
      futures.add(FutureUtils.immediateResultFuture(i % 2 == 1));
    }
    
    assertEquals(TEST_QTY, FutureUtils.countFuturesWithResult(futures, false));
  }
  
  @Test
  public void countFuturesWithResultWithTimeoutTest() throws InterruptedException, TimeoutException {
    List<ListenableFuture<Boolean>> futures = new ArrayList<>(TEST_QTY * 2);
    for (int i = 0; i < TEST_QTY * 2; i++) {
      futures.add(FutureUtils.immediateResultFuture(i % 2 == 1));
    }
    
    assertEquals(TEST_QTY, FutureUtils.countFuturesWithResult(futures, false, 100));
  }
  
  @Test (expected = TimeoutException.class)
  public void countFuturesWithResultWithTimeoutTimeoutTest() throws InterruptedException, TimeoutException {
    List<? extends ListenableFuture<?>> futures = Collections.singletonList(new SettableListenableFuture<>());
    
    assertEquals(TEST_QTY, FutureUtils.countFuturesWithResult(futures, false, 100));
    fail("Exception should have thrown");
  }
  
  @Test (expected = TimeoutException.class)
  public void countFuturesWithResultWithTimeoutZeroTimeoutTest() throws InterruptedException, TimeoutException {
    List<? extends ListenableFuture<?>> futures = Collections.singletonList(new SettableListenableFuture<>());
    
    assertEquals(TEST_QTY, FutureUtils.countFuturesWithResult(futures, false, 0));
    fail("Exception should have thrown");
  }
  
  private static void verifyCompleteFuture(final ListenableFuture<?> f, 
                                           final List<? extends ListenableFuture<?>> futures) throws InterruptedException, TimeoutException {
    final AsyncVerifier av = new AsyncVerifier();
    
    f.listener(new Runnable() {
      @Override
      public void run() {
        av.assertTrue(f.isDone());
        
        Iterator<? extends ListenableFuture<?>> it = futures.iterator();
        while (it.hasNext()) {
          av.assertTrue(it.next().isDone());
        }
        
        av.signalComplete();
      }
    });
    
    av.waitForTest();
  }
  
  @Test
  public void invokeAfterAllCompleteNullTest() {
    TestRunnable tr = new TestRunnable();
    
    FutureUtils.invokeAfterAllComplete(null, tr);
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void invokeAfterAllCompleteEmptyListTest() {
    List<ListenableFuture<?>> futures = Collections.emptyList();
    TestRunnable tr = new TestRunnable();
    
    FutureUtils.invokeAfterAllComplete(futures, tr);
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void invokeAfterSingleCompleteTest() {
    List<SettableListenableFuture<Void>> futures = 
        Collections.singletonList(new SettableListenableFuture<>());
    TestRunnable tr = new TestRunnable();
    
    FutureUtils.invokeAfterAllComplete(futures, tr);
    
    assertEquals(0, tr.getRunCount());
    
    futures.get(0).setResult(null);
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void invokeAfterAllCompleteTest() {
    SettableListenableFuture<Void> slf1 = new SettableListenableFuture<>();
    SettableListenableFuture<Void> slf2 = new SettableListenableFuture<>();
    List<SettableListenableFuture<Void>> futures = new ArrayList<>();
    futures.add(slf1);
    futures.add(slf2);
    TestRunnable tr = new TestRunnable();
    
    FutureUtils.invokeAfterAllComplete(futures, tr);
    
    assertEquals(0, tr.getRunCount());
    slf1.setResult(null);
    assertEquals(0, tr.getRunCount());
    slf2.setResult(null);
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void invokeAfterAllCompleteOnExecutorTest() {
    TestableScheduler scheduler = new TestableScheduler();
    List<SettableListenableFuture<Void>> futures = 
        Collections.singletonList(new SettableListenableFuture<>());
    TestRunnable tr = new TestRunnable();
    
    FutureUtils.invokeAfterAllComplete(futures, tr, scheduler);
    
    assertEquals(0, tr.getRunCount());
    
    futures.get(0).setResult(null);
    
    assertEquals(0, tr.getRunCount());
    
    assertEquals(1, scheduler.tick());
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void makeFirstResultFutureTest() throws InterruptedException, ExecutionException {
    makeFirstResultFutureTest(false);
  }

  @Test
  public void makeFirstResultFutureIgnoreErrorWithoutErrorTest() throws InterruptedException, ExecutionException {
    makeFirstResultFutureTest(true);
  }
  
  private static void makeFirstResultFutureTest(boolean ignoreError) throws InterruptedException, ExecutionException {
    SettableListenableFuture<String> slf = new SettableListenableFuture<>();
    List<ListenableFuture<String>> futureList = new ArrayList<>();
    futureList.add(new SettableListenableFuture<>());
    futureList.add(slf);
    futureList.add(new SettableListenableFuture<>());
    
    ListenableFuture<String> firstResult = FutureUtils.makeFirstResultFuture(futureList, ignoreError, false);
    slf.setResult(StringUtils.makeRandomString(5));
    
    assertTrue(firstResult.isDone());
    assertEquals(slf.get(), firstResult.get());
    
    for (ListenableFuture<?> lf : futureList) {
      assertTrue(lf.isDone());
      if (lf != slf) {
        assertTrue(lf.isCancelled());
      }
    }
  }
  
  @Test
  public void makeFirstResultFutureIgnoreErrorTest() throws InterruptedException, ExecutionException {
    SettableListenableFuture<String> slf = new SettableListenableFuture<>();
    SettableListenableFuture<String> errorSlf = new SettableListenableFuture<>();
    List<ListenableFuture<String>> futureList = new ArrayList<>();
    futureList.add(new SettableListenableFuture<>());
    futureList.add(slf);
    futureList.add(new SettableListenableFuture<>());
    futureList.add(errorSlf);
    futureList.add(new SettableListenableFuture<>());
    
    ListenableFuture<String> firstResult = FutureUtils.makeFirstResultFuture(futureList, true, false);
    errorSlf.setFailure(new Exception());
    
    assertFalse(firstResult.isDone());
    
    slf.setResult(StringUtils.makeRandomString(5));
    
    assertTrue(firstResult.isDone());
    assertEquals(slf.get(), firstResult.get());
  }
  
  @Test
  public void makeFirstResultFutureIgnoreErrorFinalErrorTest() throws InterruptedException, TimeoutException {
    Exception error = new Exception();
    SettableListenableFuture<String> errorSlf = new SettableListenableFuture<>();
    List<ListenableFuture<String>> futureList = new ArrayList<>();
    futureList.add(errorSlf);
    
    ListenableFuture<String> firstResult = FutureUtils.makeFirstResultFuture(futureList, true, false);
    errorSlf.setFailure(error);
    
    assertTrue(firstResult.isDone());
    AsyncVerifier av = new AsyncVerifier();
    firstResult.resultCallback((ignored) -> av.fail("Completed with result"));
    firstResult.failureCallback((t) -> {
      av.assertTrue(t == error);
      av.signalComplete();
    });
    av.waitForTest(); // technically should already be done
  }
  
  @Test
  public void makeFirstResultFutureErrorTest() throws InterruptedException, TimeoutException {
    Exception error = new Exception();
    SettableListenableFuture<String> errorSlf = new SettableListenableFuture<>();
    List<ListenableFuture<String>> futureList = new ArrayList<>();
    futureList.add(new SettableListenableFuture<>());
    futureList.add(errorSlf);
    futureList.add(new SettableListenableFuture<>());
    
    ListenableFuture<String> firstResult = FutureUtils.makeFirstResultFuture(futureList, false, false);
    errorSlf.setFailure(error);
    
    assertTrue(firstResult.isDone());
    AsyncVerifier av = new AsyncVerifier();
    firstResult.resultCallback((ignored) -> av.fail("Completed with result"));
    firstResult.failureCallback((t) -> {
      av.assertTrue(t == error);
      av.signalComplete();
    });
    av.waitForTest(); // technically should already be done
  }
  
  @Test
  public void makeCompleteFutureNullTest() {
    ListenableFuture<?> f = FutureUtils.makeCompleteFuture((Iterable<ListenableFuture<?>>)null);
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void makeCompleteFutureEmptyListTest() throws InterruptedException, ExecutionException {
    List<ListenableFuture<?>> futures = Collections.emptyList();
    ListenableFuture<?> f = FutureUtils.makeCompleteFuture(futures);
    
    assertTrue(f.isDone());
    assertNull(f.get());
  }
  
  @Test
  public void makeCompleteFutureSingletonListTest() {
    List<ListenableFuture<?>> futures = Collections.singletonList(FutureUtils.immediateResultFuture(null));
    ListenableFuture<?> f = FutureUtils.makeCompleteFuture(futures);
    
    assertTrue(f == futures.get(0));
  }
  
  @Test
  public void makeCompleteFutureEmptyCollectionTest() throws InterruptedException, ExecutionException {
    ListenableFuture<?> f = FutureUtils.makeCompleteFuture(Collections.emptySet());
    
    assertTrue(f.isDone());
    assertNull(f.get());
  }
  
  @Test
  public void makeCompleteFutureSinletonCollectionTest() {
    ListenableFuture<?> singletonFuture = FutureUtils.immediateResultFuture(null);
    ListenableFuture<?> f = FutureUtils.makeCompleteFuture(Collections.singleton(singletonFuture));
    
    assertTrue(f == singletonFuture);
  }
  
  @Test
  public void makeCompleteFutureTest() throws InterruptedException, TimeoutException, ExecutionException {
    makeCompleteFutureTest(-1);
  }
  
  @Test
  public void makeCompleteFutureWithErrorTest() throws InterruptedException, TimeoutException, ExecutionException {
    makeCompleteFutureTest(TEST_QTY / 2);
  }
  
  private static void makeCompleteFutureTest(int errorIndex) throws InterruptedException, TimeoutException, ExecutionException {
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, errorIndex);

    ListenableFuture<?> f = FutureUtils.makeCompleteFuture(futures);
    
    verifyCompleteFuture(f, futures);
    assertNull(f.get());
  }
  
  @Test
  public void makeCompleteFutureCancelTest() {
    SettableListenableFuture<?> slf = new SettableListenableFuture<>();
    assertTrue(FutureUtils.makeCompleteFuture(Collections.singletonList(slf)).cancel(true));
    
    assertTrue(slf.isCancelled());
  }
  
  @Test
  public void makeCompleteFutureWithResultNullTest() throws InterruptedException, ExecutionException {
    String result = StringUtils.makeRandomString(5);
    ListenableFuture<String> f = FutureUtils.makeCompleteFuture(null, result);
    
    assertTrue(f.isDone());
    assertEquals(result, f.get());
  }
  
  @Test
  public void makeCompleteFutureWithResultEmptyListTest() throws InterruptedException, ExecutionException {
    String result = StringUtils.makeRandomString(5);
    List<ListenableFuture<?>> futures = Collections.emptyList();
    ListenableFuture<String> f = FutureUtils.makeCompleteFuture(futures, result);
    
    assertTrue(f.isDone());
    assertEquals(result, f.get());
  }
  
  @Test
  public void makeCompleteFutureWithResultAlreadyDoneTest() throws InterruptedException, TimeoutException, ExecutionException {
    String result = StringUtils.makeRandomString(5);
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, -1);
    
    ListenableFuture<String> f = FutureUtils.makeCompleteFuture(futures, result);
    
    verifyCompleteFuture(f, futures);
    assertEquals(result, f.get());
  }
  
  @Test
  public void makeCompleteFutureWithResultNotCompleteTest() throws InterruptedException, TimeoutException, ExecutionException {
    String result = StringUtils.makeRandomString(5);
    List<SettableListenableFuture<?>> futures = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> future = new SettableListenableFuture<>();
      if (i != 0) {
        future.setResult(null);
      }
      futures.add(future);
    }
    
    ListenableFuture<String> f = FutureUtils.makeCompleteFuture(futures, result);
    futures.get(0).setResult(null); // complete now
    
    verifyCompleteFuture(f, futures);
    assertEquals(result, f.get());
  }
  
  @Test
  public void makeCompleteFutureWithNullResultTest() throws InterruptedException, TimeoutException, ExecutionException {
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, -1);
    
    ListenableFuture<?> f = FutureUtils.makeCompleteFuture(futures, null);
    
    verifyCompleteFuture(f, futures);
    assertNull(f.get());
  }
  
  @Test
  public void makeCompleteFutureWithResultCancelTest() {
    SettableListenableFuture<?> slf = new SettableListenableFuture<>();
    assertTrue(FutureUtils.makeCompleteFuture(Collections.singletonList(slf), null).cancel(true));
    
    assertTrue(slf.isCancelled());
  }
  
  @Test
  public void makeFailurePropagatingCompleteFutureNullTest() {
    ListenableFuture<?> f = 
        FutureUtils.makeFailurePropagatingCompleteFuture((Iterable<ListenableFuture<?>>)null);
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void makeFailurePropagatingCompleteFutureEmptyListTest() {
    List<ListenableFuture<?>> futures = Collections.emptyList();
    ListenableFuture<?> f = FutureUtils.makeFailurePropagatingCompleteFuture(futures);
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void makeFailurePropagatingCompleteFutureAlreadyDoneFuturesTest() {
    List<ListenableFuture<?>> futures = new ArrayList<>(TEST_QTY);
    
    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> future = new SettableListenableFuture<>();
      future.setResult(null);
      futures.add(future);
    }

    ListenableFuture<?> f = FutureUtils.makeFailurePropagatingCompleteFuture(futures);
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void makeFailurePropagatingCompleteFutureTest() throws InterruptedException, TimeoutException, ExecutionException {
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, -1);

    ListenableFuture<?> f = FutureUtils.makeFailurePropagatingCompleteFuture(futures);
    
    verifyCompleteFuture(f, futures);
    assertNull(f.get());
  }
  
  @Test
  public void makeFailurePropagatingCompleteFuturePropagateFailureTest() {
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, TEST_QTY / 2);
    ListenableFuture<?> f = FutureUtils.makeFailurePropagatingCompleteFuture(futures);
    
    try {
      f.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      // expected
    } catch (InterruptedException e) {
      fail("Interrupted?");
    }
  }
  
  @Test
  public void makeFailurePropagatingCompleteFuturePropagateCancelTest() {
    SettableListenableFuture<?> slf = new SettableListenableFuture<>();
    assertTrue(slf.cancel(false));
    ListenableFuture<?> f = FutureUtils.makeFailurePropagatingCompleteFuture(Collections.singletonList(slf));
    
    assertTrue(f.isDone());
    assertTrue(f.isCancelled());
  }
  
  @Test
  public void makeFailurePropagatingCompleteFutureCancelTest() {
    SettableListenableFuture<?> slf = new SettableListenableFuture<>();
    assertTrue(FutureUtils.makeFailurePropagatingCompleteFuture(Collections.singletonList(slf)).cancel(true));
    
    assertTrue(slf.isCancelled());
  }
  
  @Test
  public void makeFailurePropagatingCompleteFutureWithResultNullTest() throws InterruptedException, ExecutionException {
    String result = StringUtils.makeRandomString(5);
    ListenableFuture<String> f = FutureUtils.makeFailurePropagatingCompleteFuture(null, result);
    
    assertTrue(f.isDone());
    assertEquals(result, f.get());
  }
  
  @Test
  public void makeFailurePropagatingCompleteFutureWithResultEmptyListTest() throws InterruptedException, ExecutionException {
    String result = StringUtils.makeRandomString(5);
    List<ListenableFuture<?>> futures = Collections.emptyList();
    ListenableFuture<String> f = FutureUtils.makeFailurePropagatingCompleteFuture(futures, result);
    
    assertTrue(f.isDone());
    assertEquals(result, f.get());
  }
  
  @Test
  public void makeFailurePropagatingCompleteFutureWithResultAlreadyDoneTest() throws InterruptedException, TimeoutException, ExecutionException {
    String result = StringUtils.makeRandomString(5);
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, -1);
    
    ListenableFuture<String> f = FutureUtils.makeFailurePropagatingCompleteFuture(futures, result);
    
    verifyCompleteFuture(f, futures);
    assertEquals(result, f.get());
  }
  
  @Test
  public void makeFailurePropagatingCompleteFutureWithResultNotCompleteTest() throws InterruptedException, TimeoutException, ExecutionException {
    String result = StringUtils.makeRandomString(5);
    List<SettableListenableFuture<?>> futures = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> future = new SettableListenableFuture<>();
      if (i != 0) {
        future.setResult(null);
      }
      futures.add(future);
    }
    
    ListenableFuture<String> f = FutureUtils.makeFailurePropagatingCompleteFuture(futures, result);
    futures.get(0).setResult(null);
    
    verifyCompleteFuture(f, futures);
    assertEquals(result, f.get());
  }
  
  @Test
  public void makeFailurePropagatingCompleteFutureWithNullResultTest() throws InterruptedException, TimeoutException, ExecutionException {
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, -1);
    
    ListenableFuture<?> f = FutureUtils.makeFailurePropagatingCompleteFuture(futures, null);
    
    verifyCompleteFuture(f, futures);
    assertNull(f.get());
  }
  
  @Test
  public void makeFailurePropagatingCompleteFutureWithResultCancelTest() {
    SettableListenableFuture<?> slf = new SettableListenableFuture<>();
    assertTrue(FutureUtils.makeFailurePropagatingCompleteFuture(Collections.singletonList(slf), null).cancel(true));
    
    assertTrue(slf.isCancelled());
  }
  
  @Test
  public void makeCompleteListFutureNullTest() {
    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeCompleteListFuture(null);
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void makeCompleteListFutureEmptyListTest() {
    List<ListenableFuture<?>> futures = Collections.emptyList();
    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeCompleteListFuture(futures);
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void makeCompleteListFutureAlreadyDoneFuturesTest() throws InterruptedException, ExecutionException {
    List<ListenableFuture<?>> futures = new ArrayList<>(TEST_QTY);
    
    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> future = new SettableListenableFuture<>();
      if (i == TEST_QTY / 2) {
        future.setFailure(null);
      } else {
        future.setResult(null);
      }
      futures.add(future);
    }

    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeCompleteListFuture(futures);
    
    assertTrue(f.isDone());
    
    verifyAllIncluded(futures, f.get(), null);
  }
  
  private static void verifyAllIncluded(List<ListenableFuture<?>> expected, 
                                        List<ListenableFuture<?>> result, 
                                        ListenableFuture<?> excludedFuture) {
    Iterator<ListenableFuture<?>> it = expected.iterator();
    while (it.hasNext()) {
      ListenableFuture<?> f = it.next();
      if (f != excludedFuture) {
        assertTrue(result.contains(f));
      }
    }
    
    assertFalse(result.contains(excludedFuture));
    assertEquals(expected.size() - (excludedFuture == null ? 0 : 1), result.size());
  }
  
  @Test
  public void makeCompleteListFutureTest() throws InterruptedException, TimeoutException, ExecutionException {
    makeCompleteListFutureTest(-1);
  }
  
  @Test
  public void makeCompleteListFutureWithErrorTest() throws InterruptedException, 
                                                           TimeoutException, ExecutionException {
    makeCompleteListFutureTest(TEST_QTY / 2);
  }
  
  private static void makeCompleteListFutureTest(int errorIndex) throws InterruptedException, 
                                                                        TimeoutException, ExecutionException {
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, errorIndex);

    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeCompleteListFuture(futures);
    
    verifyCompleteFuture(f, futures);
    
    verifyAllIncluded(futures, f.get(), null);
  }
  
  @Test
  public void makeCompleteListFutureCancelTest() {
    SettableListenableFuture<?> slf = new SettableListenableFuture<>();
    assertTrue(FutureUtils.makeCompleteListFuture(Collections.singletonList(slf)).cancel(true));
    
    assertTrue(slf.isCancelled());
  }
  
  @Test
  public void makeSuccessListFutureNullTest() {
    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeSuccessListFuture(null);
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void makeSuccessListFutureEmptyListTest() {
    List<ListenableFuture<?>> futures = Collections.emptyList();
    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeSuccessListFuture(futures);
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void makeSuccessListFutureAlreadyDoneFuturesTest() throws InterruptedException, ExecutionException {
    List<ListenableFuture<?>> futures = new ArrayList<>(TEST_QTY);
    ListenableFuture<?> failureFuture = null;
    
    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> future = new SettableListenableFuture<>();
      if (i == TEST_QTY / 2) {
        failureFuture = future;
        future.setFailure(null);
      } else {
        future.setResult(null);
      }
      futures.add(future);
    }

    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeSuccessListFuture(futures);
    
    assertTrue(f.isDone());
    
    verifyAllIncluded(futures, f.get(), failureFuture);
  }
  
  @Test
  public void makeSuccessListFutureWithErrorTest() throws ExecutionException, InterruptedException, TimeoutException {
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, -1);
    SettableListenableFuture<?> failureFuture = new SettableListenableFuture<>();
    failureFuture.setFailure(null);
    futures.add(failureFuture);

    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeSuccessListFuture(futures);
    
    verifyCompleteFuture(f, futures);
    
    verifyAllIncluded(futures, f.get(), failureFuture);
  }
  
  @Test
  public void makeSuccessListFutureWithCancelErrorTest() throws ExecutionException, InterruptedException, TimeoutException {
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, -1);
    SettableListenableFuture<?> cancelFuture = new SettableListenableFuture<>();
    cancelFuture.cancel(false);
    futures.add(cancelFuture);

    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeSuccessListFuture(futures);
    
    verifyCompleteFuture(f, futures);
    
    verifyAllIncluded(futures, f.get(), cancelFuture);
  }
  
  @Test
  public void makeSuccessListFutureCancelTest() {
    SettableListenableFuture<?> slf = new SettableListenableFuture<>();
    assertTrue(FutureUtils.makeSuccessListFuture(Collections.singletonList(slf)).cancel(true));
    
    assertTrue(slf.isCancelled());
  }
  
  @Test
  public void makeSuccessListOrderTest() throws InterruptedException, ExecutionException {
    List<String> expectedResults = new ArrayList<>(TEST_QTY);
    List<SettableListenableFuture<String>> futures = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<String> slf = new SettableListenableFuture<>();
      if (i == 1) {
        slf.setFailure(new Exception());
      } else {
        String result = StringUtils.makeRandomString(5);
        expectedResults.add(result);
        if (i % 2 == 0) {
          slf.setResult(result);
        }
      }
      futures.add(slf);
    }
    
    ListenableFuture<List<ListenableFuture<? extends String>>> collectionFuture = 
        FutureUtils.makeSuccessListFuture(futures);

    for (int i = 2; i < TEST_QTY; i++) {
      SettableListenableFuture<String> slf = futures.get(i);
      if (! slf.isDone()) {
        slf.setResult(expectedResults.get(i - 1));
      }
    }
    
    List<ListenableFuture<? extends String>> actualResults = collectionFuture.get();
    
    assertEquals(expectedResults.size(), actualResults.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      assertEquals(expectedResults.get(i), actualResults.get(i).get());
    }
  }
  
  @Test
  public void makeFailureListFutureNullTest() {
    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeFailureListFuture(null);
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void makeFailureListFutureEmptyListTest() {
    List<ListenableFuture<?>> futures = Collections.emptyList();
    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeFailureListFuture(futures);
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void makeFailureListFutureAlreadyDoneFuturesTest() throws InterruptedException, ExecutionException {
    List<ListenableFuture<?>> futures = new ArrayList<>(TEST_QTY);
    ListenableFuture<?> failureFuture = null;
    
    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> future = new SettableListenableFuture<>();
      if (i == TEST_QTY / 2) {
        failureFuture = future;
        future.setFailure(null);
      } else {
        future.setResult(null);
      }
      futures.add(future);
    }

    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeFailureListFuture(futures);
    
    assertTrue(f.isDone());
    
    verifyNoneIncluded(futures, f.get(), failureFuture);
  }
  
  private static void verifyNoneIncluded(List<ListenableFuture<?>> exempt, 
                                         List<ListenableFuture<?>> result, 
                                         ListenableFuture<?> includedFuture) {
    Iterator<ListenableFuture<?>> it = exempt.iterator();
    while (it.hasNext()) {
      ListenableFuture<?> f = it.next();
      if (f != includedFuture) {
        assertFalse(result.contains(f));
      }
    }
    
    assertTrue(result.contains(includedFuture));
  }
  
  @Test
  public void makeFailureListFutureWithErrorTest() throws InterruptedException, 
                                                          TimeoutException, ExecutionException {
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, -1);
    SettableListenableFuture<?> failureFuture = new SettableListenableFuture<>();
    failureFuture.setFailure(null);
    futures.add(failureFuture);

    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeFailureListFuture(futures);
    
    verifyCompleteFuture(f, futures);
    
    verifyNoneIncluded(futures, f.get(), failureFuture);
  }
  
  @Test
  public void makeFailureListFutureWithCancelErrorTest() throws InterruptedException, 
                                                                TimeoutException, ExecutionException {
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, -1);
    SettableListenableFuture<?> cancelFuture = new SettableListenableFuture<>();
    cancelFuture.cancel(false);
    futures.add(cancelFuture);

    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeFailureListFuture(futures);
    
    verifyCompleteFuture(f, futures);
    
    verifyNoneIncluded(futures, f.get(), cancelFuture);
  }
  
  @Test
  public void makeFailureListFutureCancelTest() {
    SettableListenableFuture<?> slf = new SettableListenableFuture<>();
    assertTrue(FutureUtils.makeFailureListFuture(Collections.singletonList(slf)).cancel(true));
    
    assertTrue(slf.isCancelled());
  }
  
  @Test
  public void makeResultListFutureNullFuturesTest() throws InterruptedException, ExecutionException {
    ListenableFuture<List<String>> resultFuture = FutureUtils.makeResultListFuture(null, false);
    
    assertTrue(resultFuture.isDone());
    assertNotNull(resultFuture.get());
    assertTrue(resultFuture.get().isEmpty());
  }
  
  @Test
  public void makeResultListFutureAlreadyDoneFuturesTest() throws InterruptedException, ExecutionException {
    List<ListenableFuture<? extends String>> futures = makeFutures(TEST_QTY, -1);
    
    ListenableFuture<List<String>> resultFuture = FutureUtils.makeResultListFuture(futures, false);
    
    assertTrue(resultFuture.isDone());
    assertEquals(TEST_QTY, resultFuture.get().size());
  }
  
  @Test
  public void makeResultListFutureIgnoreFailureTest() throws InterruptedException, ExecutionException {
    List<ListenableFuture<? extends String>> futures = makeFutures(TEST_QTY, TEST_QTY / 2);
    
    ListenableFuture<List<String>> resultFuture = FutureUtils.makeResultListFuture(futures, true);
    
    assertTrue(resultFuture.isDone());
    assertEquals(TEST_QTY - 1, resultFuture.get().size());
  }
  
  @Test (expected = ExecutionException.class)
  public void makeResultListFutureWithFailureTest() throws InterruptedException, ExecutionException {
    List<ListenableFuture<? extends String>> futures = makeFutures(TEST_QTY, TEST_QTY / 2);
    
    ListenableFuture<List<String>> resultFuture = FutureUtils.makeResultListFuture(futures, false);
    
    assertTrue(resultFuture.isDone());
    resultFuture.get();
    fail("Exception should have thrown");
  }
  
  @Test
  public void makeResultListFutureCancelTest() {
    SettableListenableFuture<?> slf = new SettableListenableFuture<>();
    assertTrue(FutureUtils.makeResultListFuture(Collections.singletonList(slf), true).cancel(true));
    
    assertTrue(slf.isCancelled());
  }
  
  @Test
  public void makeResultListFutureResultsTest() throws InterruptedException, ExecutionException {
    List<String> expectedResults = new ArrayList<>(TEST_QTY);
    List<ListenableFuture<String>> futures = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String result = StringUtils.makeRandomString(5);
      expectedResults.add(result);
      futures.add(FutureUtils.immediateResultFuture(result));
    }
    
    List<String> actualResults = FutureUtils.makeResultListFuture(futures, false).get();
   
    assertTrue(actualResults.containsAll(expectedResults));
    assertTrue(expectedResults.containsAll(actualResults));
  }
  
  @Test
  public void makeResultListOrderTest() throws InterruptedException, ExecutionException {
    List<String> expectedResults = new ArrayList<>(TEST_QTY);
    List<SettableListenableFuture<String>> futures = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String result = StringUtils.makeRandomString(5);
      expectedResults.add(result);
      SettableListenableFuture<String> slf = new SettableListenableFuture<>();
      if (i % 2 == 1) {
        slf.setResult(result);
      }
      futures.add(slf);
    }
    
    ListenableFuture<List<String>> collectionFuture = FutureUtils.makeResultListFuture(futures, false);

    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<String> slf = futures.get(i);
      if (! slf.isDone()) {
        slf.setResult(expectedResults.get(i));
      }
    }
    
    List<String> actualResults = collectionFuture.get();

    for (int i = 0; i < TEST_QTY; i++) {
      assertEquals(expectedResults.get(i), actualResults.get(i));
    }
  }
  
  @Test
  public void cancelIncompleteFuturesTest() throws InterruptedException, ExecutionException {
    List<SettableListenableFuture<?>> futures = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> slf = new SettableListenableFuture<>();
      futures.add(slf);
      if (i % 2 == 0) {
        slf.setResult(null);
      }
    }
    
    FutureUtils.cancelIncompleteFutures(futures, false);

    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> slf = futures.get(i);
      assertTrue(slf.isDone());
      if (i % 2 == 0) {
        slf.get();
        // should not throw as was completed normally
      } else {
        assertTrue(slf.isCancelled());
      }
    }
  }
  
  @Test
  public void cancelIncompleteFuturesIfAnyFailTest() throws InterruptedException, ExecutionException {
    List<SettableListenableFuture<?>> futures = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> slf = new SettableListenableFuture<>();
      futures.add(slf);
      if (i % 2 == 0) {
        slf.setResult(null);
      }
    }
    
    FutureUtils.cancelIncompleteFuturesIfAnyFail(false, futures, false);
    
    // fail one future
    futures.get(1).setFailure(null);

    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> slf = futures.get(i);
      assertTrue(slf.isDone());
      if (i % 2 == 0) {
        slf.get();
        // should not throw as was completed normally
      } else if (i != 1) {  // skip manually failed future
        assertTrue(slf.isCancelled());
      }
    }
  }
  
  @Test
  public void cancelIncompleteFuturesIfAnyFailCancelTest() {
    List<SettableListenableFuture<?>> futures = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      futures.add(new SettableListenableFuture<>());
    }
    
    FutureUtils.cancelIncompleteFuturesIfAnyFail(false, futures, false);
    
    // cancel one future
    assertTrue(futures.get(1).cancel(false));

    for (int i = 0; i < TEST_QTY; i++) {
      assertTrue(futures.get(i).isCancelled());
    }
  }
  
  @Test
  public void cancelIncompleteFuturesIfAnyFailCopyTest() {
    List<SettableListenableFuture<?>> futures = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      futures.add(new SettableListenableFuture<>());
    }
    
    FutureUtils.cancelIncompleteFuturesIfAnyFail(true, futures, false);
    
    // copy and clear futures
    List<SettableListenableFuture<?>> futuresCopy = new ArrayList<>(futures);
    futures.clear();
    // cancel one future
    assertTrue(futuresCopy.get(1).cancel(false));

    for (int i = 0; i < TEST_QTY; i++) {
      assertTrue(futuresCopy.get(i).isCancelled());
    }
  }
  
  @Test
  public void immediateResultFutureNullResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<?> testFuture = FutureUtils.immediateResultFuture(null);
    
    ImmediateListenableFutureTest.resultTest(testFuture, null);
    assertTrue(testFuture == ImmediateResultListenableFuture.NULL_RESULT);
  }
  
  @Test
  public void immediateResultFutureEmptyStringResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<String> testFuture = FutureUtils.immediateResultFuture("");
    
    ImmediateListenableFutureTest.resultTest(testFuture, "");
    assertTrue(testFuture == ImmediateResultListenableFuture.EMPTY_STRING_RESULT);
  }
  
  @Test
  public void immediateResultFutureBooleanTrueResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<Boolean> testFuture = FutureUtils.immediateResultFuture(true);
    
    ImmediateListenableFutureTest.resultTest(testFuture, true);
    assertTrue(testFuture == ImmediateResultListenableFuture.BOOLEAN_TRUE_RESULT);
  }
  
  @Test
  public void immediateResultFutureBooleanFalseResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<Boolean> testFuture = FutureUtils.immediateResultFuture(false);
    
    ImmediateListenableFutureTest.resultTest(testFuture, false);
    assertTrue(testFuture == ImmediateResultListenableFuture.BOOLEAN_FALSE_RESULT);
  }
  
  @Test
  public void immediateResultFutureEmptyOptionalResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<Optional<String>> testFuture = FutureUtils.immediateResultFuture(Optional.empty());
    
    ImmediateListenableFutureTest.resultTest(testFuture, Optional.<String>empty());
    assertTrue(testFuture == ImmediateResultListenableFuture.EMPTY_OPTIONAL_RESULT);
    
    testFuture = FutureUtils.immediateResultFuture(Optional.of("").map((ignored) -> null));
    
    ImmediateListenableFutureTest.resultTest(testFuture, Optional.<String>empty());
    assertTrue(testFuture == ImmediateResultListenableFuture.EMPTY_OPTIONAL_RESULT);
  }
  
  @Test
  public void immediateResultFutureTest() throws InterruptedException, ExecutionException, TimeoutException {
    Object result = new Object();
    ListenableFuture<?> testFuture = FutureUtils.immediateResultFuture(result);
    
    ImmediateListenableFutureTest.resultTest(testFuture, result);
  }
  
  @Test
  public void immediateResultFutureCancelTest() {
    ListenableFuture<?> testFuture = FutureUtils.immediateResultFuture(null);
    
    ImmediateListenableFutureTest.cancelTest(testFuture);
  }
  
  @Test
  public void immediateResultFutureListenerTest() {
    ListenableFuture<?> testFuture = FutureUtils.immediateResultFuture(null);
    
    ImmediateListenableFutureTest.listenerTest(testFuture);
  }
  
  @Test
  public void immediateResultFutureCallbackTest() {
    Object result = new Object();
    ListenableFuture<?> testFuture = FutureUtils.immediateResultFuture(result);
    
    ImmediateListenableFutureTest.resultCallbackTest(testFuture, result);
  }
  
  @Test
  public void immediateFailureFutureTest() {
    Exception failure = new Exception();
    ListenableFuture<?> testFuture = FutureUtils.immediateFailureFuture(failure);
    
    ImmediateListenableFutureTest.failureTest(testFuture, failure);
  }
  
  @Test
  public void immediateFailureFutureCancelTest() {
    ListenableFuture<?> testFuture = FutureUtils.immediateFailureFuture(null);
    
    ImmediateListenableFutureTest.cancelTest(testFuture);
  }
  
  @Test
  public void immediateFailureFutureListenerTest() {
    ListenableFuture<?> testFuture = FutureUtils.immediateFailureFuture(null);
    
    ImmediateListenableFutureTest.listenerTest(testFuture);
  }
  
  @Test
  public void immediateFailureFutureCallbackTest() {
    Throwable failure = new Exception();
    ListenableFuture<?> testFuture = FutureUtils.immediateFailureFuture(failure);
    
    ImmediateListenableFutureTest.failureCallbackTest(testFuture, failure);
  }

  @Test
  public void immediateResultFutureListenerOptimizeListenerExecutorTest() throws InterruptedException, TimeoutException {
    ListenableFutureInterfaceTest.optimizeDoneListenerExecutorTest(FutureUtils.immediateResultFuture(null));
  }

  @Test
  public void immediateResultFutureDontOptimizeListenerExecutorTest() throws InterruptedException, TimeoutException {
    ListenableFutureInterfaceTest.dontOptimizeDoneListenerExecutorTest(FutureUtils.immediateResultFuture(null));
  }

  @Test
  public void immediateFailureFutureListenerOptimizeListenerExecutorTest() throws InterruptedException, TimeoutException {
    ListenableFutureInterfaceTest.optimizeDoneListenerExecutorTest(FutureUtils.immediateFailureFuture(null));
  }

  @Test
  public void immediateFailureFutureDontOptimizeListenerExecutorTest() throws InterruptedException, TimeoutException {
    ListenableFutureInterfaceTest.dontOptimizeDoneListenerExecutorTest(FutureUtils.immediateFailureFuture(null));
  }
  
  @Test
  public void scheduleWhileFirstRunInThreadTest() throws Exception {
    int scheduleDelayMillis = 10;
    TestableScheduler scheduler = new TestableScheduler();
    Optional<Object> result = Optional.of(new Object());
    ListenableFuture<?> f = FutureUtils.scheduleWhile(scheduler, scheduleDelayMillis, false, 
                                                      new Callable<Optional<Object>>() {
      private boolean first = true;
      @Override
      public Optional<Object> call() throws Exception {
        if (first) {
          first = false;
          return Optional.empty();
        } else {
          return result;
        }
      }
    }, (o) -> ! o.isPresent());

    assertFalse(f.isDone());
    assertEquals(1, scheduler.advance(scheduleDelayMillis));
    assertTrue(f.isDone());
    assertTrue(result == f.get());
  }
  
  @Test
  public void scheduleWhileFirstRunOnSchedulerTest() throws Exception {
    int scheduleDelayMillis = 10;
    TestableScheduler scheduler = new TestableScheduler();
    Optional<Object> result = Optional.of(new Object());
    ListenableFuture<?> f = FutureUtils.scheduleWhile(scheduler, scheduleDelayMillis, true, 
                                                      new Callable<Optional<Object>>() {
      private boolean first = true;
      
      @Override
      public Optional<Object> call() throws Exception {
        if (first) {
          first = false;
          return Optional.empty();
        } else {
          return result;
        }
      }
    }, (o) -> ! o.isPresent());

    assertFalse(f.isDone());
    assertEquals(1, scheduler.tick());  // first run async
    assertFalse(f.isDone());
    assertEquals(1, scheduler.advance(scheduleDelayMillis));
    assertTrue(f.isDone());
    assertTrue(result == f.get());
  }
  
  @Test
  public void scheduleWhileTaskFailureInThreadTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> f = 
        FutureUtils.scheduleWhile(scheduler, 10, false, () -> { throw failure; }, (o) -> false);
    
    assertTrue(f.isDone());
    try {
      f.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
  }
  
  @Test
  public void scheduleWhileTaskFailureOnSchedulerTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> f = 
        FutureUtils.scheduleWhile(scheduler, 10, true, () -> { throw failure; }, (o) -> false);

    assertFalse(f.isDone());
    assertEquals(1, scheduler.tick());  // first run async
    assertTrue(f.isDone());
    try {
      f.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
  }
  
  @Test
  public void scheduleWhileTimeoutWithResultTest() throws Exception {
    SingleThreadScheduler scheduler = new SingleThreadScheduler();
    try {
      ListenableFuture<Optional<?>> f = 
          FutureUtils.scheduleWhile(scheduler, 2, true, () -> Optional.empty(), 
                                    (o) -> ! o.isPresent(), DELAY_TIME, true);
      
      assertFalse(f.get(DELAY_TIME + 1_000, TimeUnit.MILLISECONDS).isPresent());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void scheduleWhileTimeoutWithExceptionTest() throws Exception {
    SingleThreadScheduler scheduler = new SingleThreadScheduler();
    try {
      ListenableFuture<Optional<?>> f = 
          FutureUtils.scheduleWhile(scheduler, 2, true, () -> Optional.empty(), 
                                    (o) -> ! o.isPresent(), DELAY_TIME, false);
      
      try {
        f.get(DELAY_TIME + 1_000, TimeUnit.MILLISECONDS);
        fail("Exception should have thrown");
      } catch (ExecutionException e) {
        // expected
        assertTrue(e.getCause() instanceof TimeoutException);
      }
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void scheduleWhilePredicateThrowsInThreadTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<Object> f = 
        FutureUtils.scheduleWhile(scheduler, 2, false, () -> null, (o) -> { throw failure; });
    
    assertTrue(f.isDone());
    try {
      f.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
  }
  
  @Test
  public void scheduleWhilePredicateThrowsOnSchedulerTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<Object> f = 
        FutureUtils.scheduleWhile(scheduler, 2, true, () -> null, (o) -> { throw failure; });

    assertFalse(f.isDone());
    assertEquals(1, scheduler.tick());
    assertTrue(f.isDone());
    try {
      f.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
  }
  
  @Test
  public void scheduleWhileAlreadyDoneWithFinalResultTest() throws Exception {
    TestableScheduler scheduler = new TestableScheduler();
    Object result = new Object();
    ListenableFuture<Object> f = 
        FutureUtils.scheduleWhile(scheduler, 2, FutureUtils.immediateResultFuture(result), 
                                  () -> result, (o) -> false);
    
    assertTrue(f.isDone());
    assertTrue(result == f.get());
  }
  
  @Test
  public void scheduleWhileAlreadyDoneWithFailureTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<Object> f = 
        FutureUtils.scheduleWhile(scheduler, 2, FutureUtils.immediateFailureFuture(failure), 
                                  () -> null, (o) -> false);
    
    assertTrue(f.isDone());
    try {
      f.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
  }
  
  @Test
  public void scheduleWhileAlreadyDoneCanceledTest() {
    TestableScheduler scheduler = new TestableScheduler();
    SettableListenableFuture<?> startingFuture = new SettableListenableFuture<>();
    startingFuture.cancel(false);
    ListenableFuture<Object> f = 
        FutureUtils.scheduleWhile(scheduler, 2, startingFuture, () -> null, (o) -> false);
    
    assertTrue(f.isDone());
    assertTrue(f.isCancelled());
  }
  
  @Test
  public void scheduleWhileCancelReturnedFutureTest() {
    TestableScheduler scheduler = new TestableScheduler();
    AtomicInteger runCount = new AtomicInteger();
    ListenableFuture<?> f = 
        FutureUtils.scheduleWhile(scheduler, 1, false, () -> {
          runCount.incrementAndGet();
          return null;
        }, (o) -> true);
    
    assertEquals(1, scheduler.advance(1));
    int startCount = runCount.get();
    f.cancel(false);
    assertEquals(1, scheduler.advance(1));  // should be task realizing it was canceled
    // verify task did not run
    assertEquals(startCount, runCount.get());
    assertEquals(0, scheduler.advance(100));  // should never run again
  }
  
  @Test
  public void runnableScheduleWhileFirstRunInThreadTest() throws Exception {
    int scheduleDelayMillis = 10;
    TestableScheduler scheduler = new TestableScheduler();
    TestRunnable tr = new TestRunnable();
    ListenableFuture<?> f = FutureUtils.scheduleWhile(scheduler, scheduleDelayMillis, false, 
                                                      tr, () -> tr.getRunCount() < 2);

    assertFalse(f.isDone());
    assertEquals(1, scheduler.advance(scheduleDelayMillis));
    assertTrue(f.isDone());
    assertNull(f.get());  // verify no error state
  }
  
  @Test
  public void runnableScheduleWhileFirstRunOnSchedulerTest() throws Exception {
    int scheduleDelayMillis = 10;
    TestableScheduler scheduler = new TestableScheduler();
    TestRunnable tr = new TestRunnable();
    ListenableFuture<?> f = FutureUtils.scheduleWhile(scheduler, scheduleDelayMillis, true, 
                                                      tr, () -> tr.getRunCount() < 2);

    assertFalse(f.isDone());
    assertEquals(1, scheduler.tick());  // first run async
    assertFalse(f.isDone());
    assertEquals(1, scheduler.advance(scheduleDelayMillis));
    assertTrue(f.isDone());
    assertNull(f.get());  // verify no error state
  }
  
  @Test
  public void runnableScheduleWhileTaskFailureInThreadTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> f = 
        FutureUtils.scheduleWhile(scheduler, 10, false, () -> { throw failure; }, () -> false);
    
    assertTrue(f.isDone());
    try {
      f.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
  }
  
  @Test
  public void runnableScheduleWhileTaskFailureOnSchedulerTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> f = 
        FutureUtils.scheduleWhile(scheduler, 10, true, () -> { throw failure; }, () -> false);

    assertFalse(f.isDone());
    assertEquals(1, scheduler.tick());  // first run async
    assertTrue(f.isDone());
    try {
      f.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
  }
  
  @Test
  public void runnableScheduleWhileTimeoutTest() throws Exception {
    SingleThreadScheduler scheduler = new SingleThreadScheduler();
    try {
      ListenableFuture<?> f = 
          FutureUtils.scheduleWhile(scheduler, 2, true, DoNothingRunnable.instance(), 
                                    () -> true, DELAY_TIME);
      
      try {
        f.get(DELAY_TIME + 1_000, TimeUnit.MILLISECONDS);
        fail("Exception should have thrown");
      } catch (ExecutionException e) {
        // expected
        assertTrue(e.getCause() instanceof TimeoutException);
      }
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void runnableScheduleWhilePredicateThrowsInThreadTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> f = 
        FutureUtils.scheduleWhile(scheduler, 2, false, DoNothingRunnable.instance(), () -> { throw failure; });
    
    assertTrue(f.isDone());
    try {
      f.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
  }
  
  @Test
  public void runnableScheduleWhilePredicateThrowsOnSchedulerTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> f = 
        FutureUtils.scheduleWhile(scheduler, 2, true, DoNothingRunnable.instance(), () -> { throw failure; });

    assertFalse(f.isDone());
    assertEquals(1, scheduler.tick());
    assertTrue(f.isDone());
    try {
      f.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
  }
  
  @Test
  public void scheduleWhileGetRunningStackTraceTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    BlockingTestRunnable btr = new BlockingTestRunnable();
    ListenableFuture<Boolean> future = 
        FutureUtils.scheduleWhile(sts, 0, 
                                  sts.submitScheduled(() -> { return true; }, DELAY_TIME), 
                                  () -> { 
                                    btr.run();
                                    return false;
                                  }, 
                                  (loop) -> loop);
    try {
      btr.blockTillStarted();
      
      StackTraceElement[] stack = future.getRunningStackTrace();
      assertNotNull(stack);
      assertEquals(BlockingTestRunnable.class.getName(), stack[2].getClassName());
    } finally {
      btr.unblock();
      sts.shutdown();
    }
  }
  
  @Test
  public void executeWhileTest() throws InterruptedException, ExecutionException {
    AtomicInteger ai = new AtomicInteger();
    ListenableFuture<Integer> f = 
        FutureUtils.executeWhile(() -> FutureUtils.immediateResultFuture(ai.getAndIncrement()), 
                                 (i) -> i < 10);

    assertTrue(f.isDone());
    assertEquals(10, f.get().intValue());
  }
  
  @Test
  public void executeWhileThreadedTest() throws InterruptedException, ExecutionException {
    SingleThreadScheduler scheduler = new SingleThreadScheduler();
    try {
      AtomicInteger ai = new AtomicInteger();
      ListenableFuture<Integer> f = 
          FutureUtils.executeWhile(() -> scheduler.submitScheduled(ai::getAndIncrement, 1), 
                                   (i) -> i < 10);
  
      assertEquals(10, f.get().intValue());
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void executeWhileTaskFailureTest() throws InterruptedException {
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> f = 
        FutureUtils.executeWhile(FutureUtils.immediateResultFuture(null), 
                                 () -> { throw failure; }, 
                                 (o) -> true);

    assertTrue(f.isDone());
    try {
      f.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
  }
  
  @Test
  public void executeWhileTimeoutWithResultTest() throws Exception {
    ListenableFuture<Optional<?>> f = 
        FutureUtils.executeWhile(() -> FutureUtils.immediateResultFuture(Optional.empty()), 
                                 (o) -> ! o.isPresent(), DELAY_TIME, true);
    assertNotNull(f.get());
    assertFalse(f.get(DELAY_TIME + 1_000, TimeUnit.MILLISECONDS).isPresent());
  }
  
  @Test
  public void executeWhileTimeoutWithExceptionTest() throws Exception {
    ListenableFuture<Optional<?>> f = 
        FutureUtils.executeWhile(() -> FutureUtils.immediateResultFuture(Optional.empty()), 
                                 (o) -> ! o.isPresent(), DELAY_TIME, false);
      
    try {
      f.get(DELAY_TIME + 1_000, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      // expected
      assertTrue(e.getCause() instanceof TimeoutException);
    }
  }
  
  @Test
  public void executeWhilePredicateThrowsTest() throws InterruptedException {
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<Object> f = 
        FutureUtils.executeWhile(() -> FutureUtils.immediateResultFuture(null), (o) -> { throw failure; });
    
    assertTrue(f.isDone());
    try {
      f.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
  }
  
  @Test
  public void executeWhileGetRunningStackTraceTest() {
    SettableListenableFuture<Boolean> startingFuture = new SettableListenableFuture<>();
    SettableListenableFuture<Boolean> runningFuture = new SettableListenableFuture<>();
    ListenableFuture<Boolean> future = 
        FutureUtils.executeWhile(startingFuture, 
                                 () -> runningFuture, 
                                 (loop) -> loop);
    try {
      startingFuture.setResult(true); // loop to running future
      runningFuture.setRunningThread(Thread.currentThread());

      StackTraceElement[] stack = future.getRunningStackTrace();
      assertNotNull(stack);
      assertEquals(this.getClass().getName(), stack[3].getClassName());
    } finally {
      runningFuture.setResult(false);
    }
  }
}
