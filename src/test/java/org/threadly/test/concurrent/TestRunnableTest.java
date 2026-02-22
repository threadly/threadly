package org.threadly.test.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;
import org.threadly.test.concurrent.TestCondition.ConditionTimeoutException;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class TestRunnableTest extends ThreadlyTester {
  private TestRunnable instance;
  
  @BeforeEach
  public void setup() {
    instance = new TestRunnable();
  }
  
  @AfterEach
  public void cleanup() {
    instance = null;
  }
  
  @Test
  public void setRunDelayInMillisTest() {
    assertEquals(0, instance.getRunDelayInMillis());
    instance.setRunDelayInMillis(DELAY_TIME);
    assertEquals(DELAY_TIME, instance.getRunDelayInMillis());
  }
  
  @Test
  public void runTest() {
    TestTestRunnable ttr = new TestTestRunnable();
    long start = Clock.accurateForwardProgressingMillis();
    
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
    
    long now = Clock.accurateForwardProgressingMillis();
    assertTrue(ttr.getDelayTillRun(2) <= now - start);
    assertTrue(ttr.getDelayTillRun(2) > ttr.getDelayTillFirstRun());
  }
  
  @Test
  public void blockTillRunTest() {
    TestRunnable tr = new TestRunnable() {
      private boolean firstRun = true;
      
      @Override
      public void handleRunStart() throws InterruptedException {
        if (firstRun) {
          firstRun = false;
          TestUtils.sleep(DELAY_TIME);
          run();
        }
      }
    };
    new Thread(tr).start();
    
    long startTime = Clock.accurateForwardProgressingMillis();
    tr.blockTillFinished(1000, 2);
    long endTime = Clock.accurateForwardProgressingMillis();
    
    assertTrue(endTime - startTime >= (DELAY_TIME - ALLOWED_VARIANCE));
  }
  
  @Test
  public void blockTillRunTestFail() {
      assertThrows(ConditionTimeoutException.class, () -> {
      instance.blockTillFinished(DELAY_TIME);
      });
  }
  
  @Test
  public void ranConcurrentlyTest() {
    TestRunnable notConcurrentTR = new TestRunnable();
    
    notConcurrentTR.run();
    notConcurrentTR.run();
    assertFalse(notConcurrentTR.ranConcurrently());
    
    TestRunnable concurrentTR = new TestRunnable() {
      private boolean ranOnce = false;
      
      @Override
      public void handleRunStart() {
        if (! ranOnce) {
          // used to prevent infinite recursion
          ranOnce = true;
          
          this.run();
        }
      }
    };
    
    concurrentTR.run();
    assertTrue(concurrentTR.ranConcurrently());
  }
  
  @Test
  public void currentlyRunningTest() {
    BlockingTestRunnable btr = new BlockingTestRunnable();
    
    assertFalse(btr.isRunning());
    
    new Thread(btr).start();
    try {
      btr.blockTillStarted();
      
      assertTrue(btr.isRunning());
    } finally {
      btr.unblock();
    }
  }
  
  private class TestTestRunnable extends TestRunnable {
    private boolean handleRunStartCalled = false;
    private boolean handleRunFinishCalled = false;
    private boolean startCalledBeforeFinish = false;
    
    @Override
    public void handleRunStart() {
      handleRunStartCalled = true;
    }
    
    @Override
    public void handleRunFinish() {
      handleRunFinishCalled = true;
      startCalledBeforeFinish = handleRunStartCalled;
    }
  }
}
