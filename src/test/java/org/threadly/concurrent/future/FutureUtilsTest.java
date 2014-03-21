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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.ThreadlyTestUtil;
import org.threadly.concurrent.PriorityScheduledExecutor;
import org.threadly.concurrent.StrictPriorityScheduledExecutor;
import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class FutureUtilsTest {
  private static PriorityScheduledExecutor scheduler;
  
  @BeforeClass
  public static void setupClass() {
    scheduler = new StrictPriorityScheduledExecutor(1, 1, 1000);
    
    ThreadlyTestUtil.setDefaultUncaughtExceptionHandler();
  }
  
  @AfterClass
  public static void tearDownClass() {
    scheduler.shutdownNow();
    scheduler = null;
  }
  
  private static List<ListenableFuture<?>> makeFutures(int count, int errorIndex) {
    List<ListenableFuture<?>> result = new ArrayList<ListenableFuture<?>>(count);
    
    for (int i = 0; i < count; i++) {
      TestRunnable tr;
      if (i == errorIndex) {
        tr = new TestRuntimeFailureRunnable(DELAY_TIME);
      } else {
        tr = new TestRunnable(DELAY_TIME);
      }
      result.add(scheduler.submit(tr));
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
  public void makeAllCompleteFutureNullTest() {
    ListenableFuture<?> f = FutureUtils.makeAllCompleteFuture(null);
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void makeAllCompleteFutureEmptyListTest() {
    List<ListenableFuture<?>> futures = Collections.emptyList();
    ListenableFuture<?> f = FutureUtils.makeAllCompleteFuture(futures);
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void makeAllCompleteFutureAlreadyDoneFuturesTest() {
    List<ListenableFuture<?>> futures = new ArrayList<ListenableFuture<?>>(TEST_QTY);
    
    for (int i = 0; i < TEST_QTY; i++) {
      ListenableFutureResult<?> future = new ListenableFutureResult<Object>();
      future.setResult(null);
      futures.add(future);
    }

    ListenableFuture<?> f = FutureUtils.makeAllCompleteFuture(futures);
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void makeAllCompleteFutureTest() throws InterruptedException, TimeoutException {
    makeAllCompleteFutureTest(-1);
  }
  
  @Test
  public void makeAllCompleteFutureWithErrorTest() throws InterruptedException, TimeoutException {
    makeAllCompleteFutureTest(TEST_QTY / 2);
  }
  
  private static void makeAllCompleteFutureTest(int errorIndex) throws InterruptedException, TimeoutException {
    final List<ListenableFuture<?>> futures = makeFutures(TEST_QTY, errorIndex);

    final ListenableFuture<?> f = FutureUtils.makeAllCompleteFuture(futures);
    
    final AsyncVerifier av = new AsyncVerifier();
    f.addListener(new Runnable() {
      @Override
      public void run() {
        av.assertTrue(f.isDone());
        
        Iterator<ListenableFuture<?>> it = futures.iterator();
        while (it.hasNext()) {
          assertTrue(it.next().isDone());
        }
        
        av.signalComplete();
      }
    });
    
    av.waitForTest();
  }
}
