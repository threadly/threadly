package org.threadly.concurrent;

import java.util.concurrent.Callable;

import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class TestCallable extends TestCondition 
                          implements Callable<Object> {
  private final long runDurration;
  private final long creationTime;
  private final Object result;
  private volatile long callTime;
  private volatile boolean done;
  
  public TestCallable() {
    this(0);
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