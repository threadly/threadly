package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.NoThreadScheduler.TaskContainer;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class NoThreadSchedulerTaskContainerTest {
  private NoThreadScheduler scheduler;
  
  @Before
  public void setup() {
    scheduler = new NoThreadScheduler();
  }
  
  @After
  public void cleanup() {
    scheduler = null;
  }
  
  @Test
  public void getContainedRunnableTest() {
    TestContainer tc = new TestContainer(false);
    
    assertTrue(tc.getContainedRunnable() == tc.runnable);
  }
  
  @Test
  public void runTaskTest() {
    TestContainer tc = new TestContainer(false);
    
    tc.runTask();
    
    assertTrue(tc.prepareCalled);
    assertTrue(tc.runCompleteCalled);
    assertTrue(((TestRunnable)tc.runnable).ranOnce());
  }
  
  @Test
  public void runTaskExceptionTest() {
    TestContainer tc = new TestContainer(true);
    
    try {
      tc.runTask();
      fail("Exception should have thrown");
    } catch (RuntimeException e) {
      // expected
    }
    
    assertTrue(tc.prepareCalled);
    assertTrue(tc.runCompleteCalled);
  }
  
  private class TestContainer extends TaskContainer {
    private boolean prepareCalled = false;
    private boolean runCompleteCalled = false;
    
    protected TestContainer(boolean throwException) {
      scheduler.super(throwException ? new TestRuntimeFailureRunnable() : new TestRunnable());
    }

    @Override
    protected boolean prepareForRun() {
      assertFalse(prepareCalled);
      
      prepareCalled = true;
      
      return true;
    }

    @Override
    protected void runComplete() {
      assertFalse(runCompleteCalled);
      
      runCompleteCalled = true;
    }

    @Override
    public long getRunTime() {
      throw new UnsupportedOperationException();
    }
  }
}
