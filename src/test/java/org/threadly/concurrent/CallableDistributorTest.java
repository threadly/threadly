package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.concurrent.CallableDistributor.Result;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class CallableDistributorTest {
  private static PriorityScheduledExecutor scheduler;
  
  @BeforeClass
  public static void setupClass() {
    scheduler = new PriorityScheduledExecutor(10, 10, 200);
  }
  
  @AfterClass
  public static void tearDownClass() {
    scheduler.shutdownNow();
    scheduler = null;
  }
  
  private CallableDistributor<String, String> distributor;
  
  @Before
  public void setup() {
    distributor = new CallableDistributor<String, String>(scheduler);
  }
  
  @After
  public void tearDown() {
    distributor = null;
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new CallableDistributor<String, String>((TaskExecutorDistributor)null);
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
    String key = "foo";
    List<TestCallable> callables = new ArrayList<TestCallable>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      TestCallable tc = new TestCallable(false, 0, Integer.toString(i));
      callables.add(tc);
      distributor.submit(key, tc);
    }
    
    assertTrue(distributor.waitingResults(key));
    
    List<Result<String>> results = new ArrayList<Result<String>>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
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
    String key = "foo";
    List<TestCallable> callables = new ArrayList<TestCallable>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      TestCallable tc = new TestCallable(false, 0, Integer.toString(i));
      callables.add(tc);
      distributor.submit(key, tc);
    }
    
    List<Result<String>> results = new ArrayList<Result<String>>(TEST_QTY);
    while (results.size() != TEST_QTY) {
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
