package org.threadly.test;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.test.TestCondition.TimeoutException;

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
    assertEquals(instance.getRunCount(), 0);
    assertFalse(instance.ranOnce());
  }
  
  @Test
  public void runTest() {
    TestTestRunnable ttr = new TestTestRunnable();
    long start = System.currentTimeMillis();
    
    TestUtil.blockTillClockAdvances();
    
    ttr.run();
    
    assertTrue(ttr.handleRunCalled);
    assertTrue(ttr.ranOnce());
    assertEquals(ttr.getRunCount(), 1);
    assertTrue(ttr.getDelayTillFirstRun() > 0);

    TestUtil.blockTillClockAdvances();
    
    ttr.run();
    long now = System.currentTimeMillis();
    assertTrue(ttr.getDelayTillRun(2) <= now - start);
    assertTrue(ttr.getDelayTillRun(2) > ttr.getDelayTillFirstRun());
  }
  
  @Test
  public void blockTillRunTest() {
    final int delay = 100;
    
    TestRunnable tr = new TestRunnable() {
      private boolean firstRun = true;
      
      @Override
      public void handleRun() throws InterruptedException {
        if (firstRun) {
          firstRun = false;
          TestUtil.sleep(delay);
          run();
        }
      }
    };
    new Thread(tr).start();
    
    long startTime = System.currentTimeMillis();
    tr.blockTillRun(1000, 2);
    long endTime = System.currentTimeMillis();
    
    assertTrue(endTime - startTime >= delay);
  }
  
  @Test (expected = TimeoutException.class)
  public void blockTillRunTestFail() {
    instance.blockTillRun(10);
    
    fail("Exception should have thrown");
  }
  
  private class TestTestRunnable extends TestRunnable {
    private boolean handleRunCalled = false;
    
    @Override
    public void handleRun() {
      handleRunCalled = true;
    }
  }
}
