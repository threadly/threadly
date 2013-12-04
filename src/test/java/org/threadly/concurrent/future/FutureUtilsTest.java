package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.PriorityScheduledExecutor;
import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class FutureUtilsTest {
  private static final int RUNNABLE_RUN_TIME = 10;
  
  private PriorityScheduledExecutor scheduler;
  
  @Before
  public void setup() {
    scheduler = new PriorityScheduledExecutor(1, 1, 1000);
  }
  
  @After
  public void tearDown() {
    scheduler.shutdownNow();
    scheduler = null;
  }
  
  private List<Future<?>> makeFutures(int count, int errorIndex) {
    List<Future<?>> result = new ArrayList<Future<?>>(count);
    
    for (int i = 0; i < count; i++) {
      TestRunnable tr;
      if (i == errorIndex) {
        tr = new TestRuntimeFailureRunnable(RUNNABLE_RUN_TIME);
      } else {
        tr = new TestRunnable(RUNNABLE_RUN_TIME);
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
    int futureCount = 5;
    
    List<Future<?>> futures = makeFutures(futureCount, -1);
    
    FutureUtils.blockTillAllComplete(futures);
    
    Iterator<Future<?>> it = futures.iterator();
    while (it.hasNext()) {
      assertTrue(it.next().isDone());
    }
  }
  
  @Test
  public void blockTillAllCompleteErrorTest() throws InterruptedException {
    int futureCount = 5;
    int errorIndex = 2;
    
    List<Future<?>> futures = makeFutures(futureCount, errorIndex);
    
    FutureUtils.blockTillAllComplete(futures);
    
    Iterator<Future<?>> it = futures.iterator();
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
    int futureCount = 5;
    
    List<Future<?>> futures = makeFutures(futureCount, -1);
    
    FutureUtils.blockTillAllCompleteOrFirstError(futures);
    
    Iterator<Future<?>> it = futures.iterator();
    while (it.hasNext()) {
      assertTrue(it.next().isDone());
    }
  }
  
  @Test
  public void blockTillAllCompleteOrFirstErrorErrorTest() throws InterruptedException {
    int futureCount = 5;
    int errorIndex = 2;
    
    List<Future<?>> futures = makeFutures(futureCount, errorIndex);
    
    FutureUtils.blockTillAllComplete(futures);

    Iterator<Future<?>> it = futures.iterator();
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
}
