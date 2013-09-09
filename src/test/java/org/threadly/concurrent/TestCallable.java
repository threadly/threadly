package org.threadly.concurrent;

import java.util.concurrent.Callable;

import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class TestCallable extends TestCondition 
                          implements Callable<Object> {
  private static final int RUN_CONDITION_POLL_INTERVAL = 20;
  
  private final long runDurration;
  private final long creationTime;
  private final Object result;
  private volatile long callTime;
  private volatile boolean done;
  
  public TestCallable() {
    this(0);
  }
  
  /**
   * Call to get the time recorded when the runnable was constructed.
   * 
   * @return time in milliseconds object was constructed
   */
  public long getCreationTime() {
    return creationTime;
  }
  
  /**
   * Blocks until run completed been called the provided qty of times.
   * 
   * @param timeout time to wait for run to be called
   * @param expectedRunCount run count to wait for
   */
  public void blockTillFinished(int timeout) {
    blockTillTrue(timeout, RUN_CONDITION_POLL_INTERVAL);
  }
  
  protected void handleCallStart() {
    // ignored by default
  }
  
  public TestCallable(long runDurration) {
    this.runDurration = runDurration;
    this.creationTime = System.currentTimeMillis();
    callTime = -1;
    result = new Object();
    done = false;
  }

  public long getDelayTillFirstRun() {
    return callTime - creationTime;
  }

  @Override
  public Object call() {
    callTime = System.currentTimeMillis();
    
    handleCallStart();
    
    TestUtils.sleep(runDurration);
    
    done = true;
    
    return result;
  }

  @Override
  public boolean get() {
    return done;
  }
  
  public boolean isDone() {
    return done;
  }
  
  public Object getReturnedResult() {
    return result;
  }
}