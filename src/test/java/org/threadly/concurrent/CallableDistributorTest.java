package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.CallableDistributor.Result;
import org.threadly.concurrent.lock.StripedLock;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class CallableDistributorTest {
  private PriorityScheduledExecutor executor;
  private CallableDistributor<String, String> distributor;
  
  @Before
  public void setup() {
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        // ignored
      }
    });
    
    executor = new PriorityScheduledExecutor(10, 10, 200);
    distributor = new CallableDistributor<String, String>(executor);
  }
  
  @After
  public void tearDown() {
    executor.shutdown();
    executor = null;
    distributor = null;
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new CallableDistributor<String, String>(null, new StripedLock(1));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitNullKeyFail() {
    distributor.submit(null, new TestCallable(false, 0, null));
    
    fail("Exception should have been thrown");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitNullCallableail() {
    distributor.submit("foo", null);
    
    fail("Exception should have been thrown");
  }
  
  @Test
  public void getNextResultTest() throws InterruptedException, 
                                         ExecutionException {
    String key = "foo";
    String resultStr = "foobar";
    TestCallable tc = new TestCallable(false, 50, resultStr);  // delay to force .get calls to have to wait
    distributor.submit(key, tc);
    
    assertTrue(distributor.waitingResults(key));
    
    Result<String> result = distributor.getNextResult(key);
    assertNotNull(result);
    assertTrue(result.getResult() == resultStr);
    assertNull(result.getFailure());
    assertTrue(result.get() == resultStr);
  }
  
  @Test (expected = ExecutionException.class)
  public void getNextResultExceptionTest() throws InterruptedException, 
                                                  ExecutionException {
    String key = "foo";
    String resultStr = "foobar";
    TestCallable tc = new TestCallable(true, 50, resultStr);  // delay to force .get calls to have to wait
    distributor.submit(key, tc);
    
    assertTrue(distributor.waitingResults(key));
    
    Result<String> result = distributor.getNextResult(key);
    assertNotNull(result);
    assertNull(result.getResult());
    assertTrue(result.getFailure() == tc.thrown);
    
    // will throw an exception as final verification
    assertTrue(result.get() == resultStr);
    fail("ExecutionException should have been thrown");
  }
  
  public void getNextResultMultipleTest() throws InterruptedException {
    int testQty = 10;
    String key = "foo";
    List<TestCallable> callables = new ArrayList<TestCallable>(testQty);
    for (int i = 0; i < testQty; i++) {
      TestCallable tc = new TestCallable(false, 0, Integer.toString(i));
      callables.add(tc);
      distributor.submit(key, tc);
    }
    
    assertTrue(distributor.waitingResults(key));
    
    List<Result<String>> results = new ArrayList<Result<String>>(testQty);
    for (int i = 0; i < testQty; i++) {
      Result<String> result = distributor.getNextResult(key);
      assertNotNull(result);
      assertFalse(results.contains(result));
      results.add(result);
    }
  }
  
  @Test
  public void waitingResultsFalseTest() {
    assertFalse(distributor.waitingResults("foobar"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getNextResultNullFail() throws InterruptedException {
    distributor.getAllResults(null);
    
    fail("Exception should have been thrown");
  }
  
  @Test (expected = IllegalStateException.class)
  public void getNextResultNoCallableFail() throws InterruptedException {
    distributor.getAllResults("foobar");
    
    fail("Exception should have been thrown");
  }
  
  @Test
  public void getAllResultsTest() throws InterruptedException {
    int testQty = 10;
    String key = "foo";
    List<TestCallable> callables = new ArrayList<TestCallable>(testQty);
    for (int i = 0; i < testQty; i++) {
      TestCallable tc = new TestCallable(false, 0, Integer.toString(i));
      callables.add(tc);
      distributor.submit(key, tc);
    }
    
    List<Result<String>> results = new ArrayList<Result<String>>(testQty);
    while (results.size() != testQty) {
      List<Result<String>> currResults = distributor.getAllResults(key);
      assertNotNull(currResults);
      assertFalse(currResults.isEmpty());
      Iterator<Result<String>> it = currResults.iterator();
      while (it.hasNext()) {
        assertFalse(results.contains(it.next()));
      }
      results.addAll(currResults);
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getAllResultsNullFail() throws InterruptedException {
    distributor.getAllResults(null);
    
    fail("Exception should have been thrown");
  }
  
  @Test (expected = IllegalStateException.class)
  public void getAllResultsNoCallableFail() throws InterruptedException {
    distributor.getAllResults("foobar");
    
    fail("Exception should have been thrown");
  }
  
  private static class TestCallable extends TestCondition 
                                    implements Callable<String> {
    private final boolean throwException;
    private final long delay;
    private final String result;
    private volatile RuntimeException thrown;
    private volatile boolean done;
    
    private TestCallable(boolean throwException, 
                         long delay, 
                         String result) {
      this.throwException = throwException;
      this.delay = delay;
      this.result = result;
      thrown = null;
      done = false;
    }

    @Override
    public String call() {
      TestUtils.sleep(delay);
      
      done = true;
      
      if (throwException) {
        thrown = new RuntimeException();
        throw thrown;
      }
      
      return result;
    }

    @Override
    public boolean get() {
      return done;
    }
  }
}
