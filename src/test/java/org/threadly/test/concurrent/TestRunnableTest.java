package org.threadly.test.concurrent;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.test.concurrent.TestCondition.ConditionTimeoutException;

@SuppressWarnings("javadoc")
public class TestRunnableTest {
  private TestRunnable instance;
  
  @Before
  public void setup() {
    instance = new TestRunnable();
  }
  
  @After
  public void tearDown() {
    instance = null;
  }
  
  @Test
  public void constructorTest() {
    assertEquals(0, instance.getRunCount());
    assertFalse(instance.ranOnce());
    assertFalse(instance.ranConcurrently());
    assertEquals(0, instance.getRunDelayInMillis());
    
    int delay = 10;
    instance = new TestRunnable(delay);
    assertEquals(delay, instance.getRunDelayInMillis());
  }
  
  @Test
  public void setRunDelayInMillisTest() {
    int delay = 10;
    
    assertEquals(0, instance.getRunDelayInMillis());
    instance.setRunDelayInMillis(delay);
    assertEquals(delay, instance.getRunDelayInMillis());
  }
  
  @Test
  public void runTest() {
    TestTestRunnable ttr = new TestTestRunnable();
    long start = System.currentTimeMillis();
    
    TestUtils.blockTillClockAdvances();
    
    ttr.run();
    
    assertTrue(ttr.handleRunStartCalled);
    assertTrue(ttr.handleRunFinishCalled);
    assertTrue(ttr.startCalledBeforeFinish);
    assertTrue(ttr.ranOnce());
    assertEquals(1, ttr.getRunCount());
    assertTrue(ttr.getDelayTillFirstRun() > 0);

    TestUtils.blockTillClockAdvances();
    
    ttr.run();
    
    TestUtils.blockTillClockAdvances();
    
    long now = System.currentTimeMillis();
    assertTrue(ttr.getDelayTillRun(2) <= now - start);
    assertTrue(ttr.getDelayTillRun(2) > ttr.getDelayTillFirstRun());
  }
  
  @Test
  public void runWithDelay() {
    int delay = 10;
    int runCount = 2;
    instance.setRunDelayInMillis(delay);
    
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < runCount; i++) {
      instance.run();
    }
    long endTime = System.currentTimeMillis();
    
    assertTrue(endTime - startTime >= (delay * runCount));
  }
  
  @Test
  public void blockTillRunTest() {
    final int delay = 100;
    
    TestRunnable tr = new TestRunnable() {
      private boolean firstRun = true;
      
      @Override
      protected void handleRunStart() throws InterruptedException {
        if (firstRun) {
          firstRun = false;
          TestUtils.sleep(delay);
          run();
        }
      }
    };
    new Thread(tr).start();
    
    long startTime = System.currentTimeMillis();
    tr.blockTillFinished(1000, 2);
    long endTime = System.currentTimeMillis();
    
    assertTrue(endTime - startTime >= delay);
  }
  
  @Test (expected = ConditionTimeoutException.class)
  public void blockTillRunTestFail() {
    instance.blockTillFinished(10);
    
    fail("Exception should have thrown");
  }
  
  private class TestTestRunnable extends TestRunnable {
    private boolean handleRunStartCalled = false;
    private boolean handleRunFinishCalled = false;
    private boolean startCalledBeforeFinish = false;
    
    @Override
    protected void handleRunStart() {
      handleRunStartCalled = true;
    }
    
    @Override
    protected void handleRunFinish() {
      handleRunFinishCalled = true;
      startCalledBeforeFinish = handleRunStartCalled;
    }
  }
}
