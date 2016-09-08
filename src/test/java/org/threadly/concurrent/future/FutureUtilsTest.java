package org.threadly.concurrent.future;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class FutureUtilsTest {
  private static <T> List<ListenableFuture<? extends T>> makeFutures(int count, int errorIndex) {
    List<ListenableFuture<? extends T>> result = new ArrayList<ListenableFuture<? extends T>>(count + 1);
    
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
    FutureUtils.blockTillAllComplete(null); // should return immediately
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
    List<? extends ListenableFuture<?>> futures = Collections.singletonList(new SettableListenableFuture<Void>());
    
    FutureUtils.blockTillAllComplete(futures, 100);
    fail("Exception should have thrown");
  }
  
  @Test (expected = TimeoutException.class)
  public void blockTillAllCompleteWithTimeoutZeroTimeoutTest() throws InterruptedException, TimeoutException {
    List<? extends ListenableFuture<?>> futures = Collections.singletonList(new SettableListenableFuture<Void>());
    
    FutureUtils.blockTillAllComplete(futures, 0);
    fail("Exception should have thrown");
  }
  
  @Test
  public void blockTillAllCompleteOrFirstErrorNullTest() throws InterruptedException, ExecutionException {
    FutureUtils.blockTillAllCompleteOrFirstError(null); // should return immediately
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
    List<? extends ListenableFuture<?>> futures = Collections.singletonList(new SettableListenableFuture<Void>());
    
    FutureUtils.blockTillAllCompleteOrFirstError(futures, 100);
    fail("Exception should have thrown");
  }
  
  @Test (expected = TimeoutException.class)
  public void blockTillAllCompleteOrFirstErrorWithTimeoutZeroTimeoutTest() throws InterruptedException, 
                                                                                  TimeoutException, ExecutionException {
    List<? extends ListenableFuture<?>> futures = Collections.singletonList(new SettableListenableFuture<Void>());
    
    FutureUtils.blockTillAllCompleteOrFirstError(futures, 0);
    fail("Exception should have thrown");
  }
  
  @Test
  public void countFuturesWithResultTest() throws InterruptedException {
    List<ListenableFuture<Boolean>> futures = new ArrayList<ListenableFuture<Boolean>>(TEST_QTY * 2);
    for (int i = 0; i < TEST_QTY * 2; i++) {
      futures.add(FutureUtils.immediateResultFuture(i % 2 == 1));
    }
    
    assertEquals(TEST_QTY, FutureUtils.countFuturesWithResult(futures, false));
  }
  
  @Test
  public void countFuturesWithResultWithTimeoutTest() throws InterruptedException, TimeoutException {
    List<ListenableFuture<Boolean>> futures = new ArrayList<ListenableFuture<Boolean>>(TEST_QTY * 2);
    for (int i = 0; i < TEST_QTY * 2; i++) {
      futures.add(FutureUtils.immediateResultFuture(i % 2 == 1));
    }
    
    assertEquals(TEST_QTY, FutureUtils.countFuturesWithResult(futures, false, 100));
  }
  
  @Test (expected = TimeoutException.class)
  public void countFuturesWithResultWithTimeoutTimeoutTest() throws InterruptedException, TimeoutException {
    List<? extends ListenableFuture<?>> futures = Collections.singletonList(new SettableListenableFuture<Void>());
    
    assertEquals(TEST_QTY, FutureUtils.countFuturesWithResult(futures, false, 100));
    fail("Exception should have thrown");
  }
  
  @Test (expected = TimeoutException.class)
  public void countFuturesWithResultWithTimeoutZeroTimeoutTest() throws InterruptedException, TimeoutException {
    List<? extends ListenableFuture<?>> futures = Collections.singletonList(new SettableListenableFuture<Void>());
    
    assertEquals(TEST_QTY, FutureUtils.countFuturesWithResult(futures, false, 0));
    fail("Exception should have thrown");
  }
  
  @Test
  public void makeCompleteFutureNullTest() {
    ListenableFuture<?> f = FutureUtils.makeCompleteFuture(null);
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void makeCompleteFutureEmptyListTest() {
    List<ListenableFuture<?>> futures = Collections.emptyList();
    ListenableFuture<?> f = FutureUtils.makeCompleteFuture(futures);
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void makeCompleteFutureAlreadyDoneFuturesTest() {
    List<ListenableFuture<?>> futures = new ArrayList<ListenableFuture<?>>(TEST_QTY);
    
    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> future = new SettableListenableFuture<Void>();
      future.setResult(null);
      futures.add(future);
    }

    ListenableFuture<?> f = FutureUtils.makeCompleteFuture(futures);
    
    assertTrue(f.isDone());
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
    SettableListenableFuture<?> slf = new SettableListenableFuture<Void>();
    assertTrue(FutureUtils.makeCompleteFuture(Collections.singletonList(slf)).cancel(true));
    
    assertTrue(slf.isCancelled());
  }
  
  private static void verifyCompleteFuture(final ListenableFuture<?> f, 
                                           final List<ListenableFuture<?>> futures) throws InterruptedException, TimeoutException {
    final AsyncVerifier av = new AsyncVerifier();
    
    f.addListener(new Runnable() {
      @Override
      public void run() {
        av.assertTrue(f.isDone());
        
        Iterator<ListenableFuture<?>> it = futures.iterator();
        while (it.hasNext()) {
          av.assertTrue(it.next().isDone());
        }
        
        av.signalComplete();
      }
    });
    
    av.waitForTest();
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
  public void makeCompleteFutureWithResultTest() throws InterruptedException, TimeoutException, ExecutionException {
    String result = StringUtils.makeRandomString(5);
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, -1);
    
    ListenableFuture<String> f = FutureUtils.makeCompleteFuture(futures, result);
    
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
    SettableListenableFuture<?> slf = new SettableListenableFuture<Void>();
    assertTrue(FutureUtils.makeCompleteFuture(Collections.singletonList(slf), null).cancel(true));
    
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
    List<ListenableFuture<?>> futures = new ArrayList<ListenableFuture<?>>(TEST_QTY);
    
    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> future = new SettableListenableFuture<Void>();
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
    SettableListenableFuture<?> slf = new SettableListenableFuture<Void>();
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
    List<ListenableFuture<?>> futures = new ArrayList<ListenableFuture<?>>(TEST_QTY);
    ListenableFuture<?> failureFuture = null;
    
    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> future = new SettableListenableFuture<Void>();
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
    SettableListenableFuture<?> failureFuture = new SettableListenableFuture<Void>();
    failureFuture.setFailure(null);
    futures.add(failureFuture);

    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeSuccessListFuture(futures);
    
    verifyCompleteFuture(f, futures);
    
    verifyAllIncluded(futures, f.get(), failureFuture);
  }
  
  @Test
  public void makeSuccessListFutureWithCancelErrorTest() throws ExecutionException, InterruptedException, TimeoutException {
    List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, -1);
    SettableListenableFuture<?> cancelFuture = new SettableListenableFuture<Void>();
    cancelFuture.cancel(false);
    futures.add(cancelFuture);

    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeSuccessListFuture(futures);
    
    verifyCompleteFuture(f, futures);
    
    verifyAllIncluded(futures, f.get(), cancelFuture);
  }
  
  @Test
  public void makeSuccessListFutureCancelTest() {
    SettableListenableFuture<?> slf = new SettableListenableFuture<Void>();
    assertTrue(FutureUtils.makeSuccessListFuture(Collections.singletonList(slf)).cancel(true));
    
    assertTrue(slf.isCancelled());
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
    List<ListenableFuture<?>> futures = new ArrayList<ListenableFuture<?>>(TEST_QTY);
    ListenableFuture<?> failureFuture = null;
    
    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> future = new SettableListenableFuture<Void>();
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
    SettableListenableFuture<?> failureFuture = new SettableListenableFuture<Void>();
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
    SettableListenableFuture<?> cancelFuture = new SettableListenableFuture<Void>();
    cancelFuture.cancel(false);
    futures.add(cancelFuture);

    ListenableFuture<List<ListenableFuture<?>>> f = FutureUtils.makeFailureListFuture(futures);
    
    verifyCompleteFuture(f, futures);
    
    verifyNoneIncluded(futures, f.get(), cancelFuture);
  }
  
  @Test
  public void makeFailureListFutureCancelTest() {
    SettableListenableFuture<?> slf = new SettableListenableFuture<Void>();
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
    SettableListenableFuture<?> slf = new SettableListenableFuture<Void>();
    assertTrue(FutureUtils.makeResultListFuture(Collections.singletonList(slf), true).cancel(true));
    
    assertTrue(slf.isCancelled());
  }
  
  @Test
  public void makeResultListFutureResultsTest() throws InterruptedException, ExecutionException {
    List<String> expectedResults = new ArrayList<String>(TEST_QTY);
    List<ListenableFuture<String>> futures = new ArrayList<ListenableFuture<String>>(TEST_QTY);
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
  public void cancelIncompleteFuturesTest() throws InterruptedException, ExecutionException {
    List<SettableListenableFuture<?>> futures = new ArrayList<SettableListenableFuture<?>>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> slf = new SettableListenableFuture<Void>();
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
    List<SettableListenableFuture<?>> futures = new ArrayList<SettableListenableFuture<?>>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      SettableListenableFuture<?> slf = new SettableListenableFuture<Void>();
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
    List<SettableListenableFuture<?>> futures = new ArrayList<SettableListenableFuture<?>>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      futures.add(new SettableListenableFuture<Void>());
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
    List<SettableListenableFuture<?>> futures = new ArrayList<SettableListenableFuture<?>>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      futures.add(new SettableListenableFuture<Void>());
    }
    
    FutureUtils.cancelIncompleteFuturesIfAnyFail(true, futures, false);
    
    // copy and clear futures
    List<SettableListenableFuture<?>> futuresCopy = new ArrayList<SettableListenableFuture<?>>(futures);
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
  public void immediateResultFutureAddListenerTest() {
    ListenableFuture<?> testFuture = FutureUtils.immediateResultFuture(null);
    
    ImmediateListenableFutureTest.addListenerTest(testFuture);
  }
  
  @Test
  public void immediateResultFutureAddCallbackTest() {
    Object result = new Object();
    ListenableFuture<?> testFuture = FutureUtils.immediateResultFuture(result);
    
    ImmediateListenableFutureTest.resultAddCallbackTest(testFuture, result);
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
  public void immediateFailureFutureAddListenerTest() {
    ListenableFuture<?> testFuture = FutureUtils.immediateFailureFuture(null);
    
    ImmediateListenableFutureTest.addListenerTest(testFuture);
  }
  
  @Test
  public void immediateFailureFutureAddCallbackTest() {
    Throwable failure = new Exception();
    ListenableFuture<?> testFuture = FutureUtils.immediateFailureFuture(failure);
    
    ImmediateListenableFutureTest.failureAddCallbackTest(testFuture, failure);
  }
}
