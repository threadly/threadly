package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.concurrent.AbstractPriorityScheduler.TaskWrapper;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class PrioritySchedulerTaskWrapperTest {
  @Test
  public void cancelTest() {
    TestWrapper tw = new TestWrapper();
    assertFalse(tw.canceled);
    
    tw.cancel();
    
    assertTrue(tw.canceled);
  }
  
  @Test
  public void toStringTest() {
    TestWrapper tw = new TestWrapper();
    String result = tw.toString();
    assertNotNull(result);
    assertFalse(result.isEmpty());
    assertEquals(tw.task.toString(), result);
  }
  
  private class TestWrapper extends TaskWrapper {
    private final int delayInMs;
    @SuppressWarnings("unused")
    private boolean canExecuteCalled;
    @SuppressWarnings("unused")
    private boolean runCalled;
    
    protected TestWrapper() {
      this(0);
    }
    
    protected TestWrapper(int delay) {
      this(DoNothingRunnable.instance(), delay);
    }
    
    protected TestWrapper(Runnable task, int delayInMs) {
      super(task);
      
      this.delayInMs = delayInMs;
      canExecuteCalled = false;
      runCalled = false;
    }

    @Override
    public long getPureRunTime() {
      return Clock.lastKnownForwardProgressingMillis() + delayInMs;
    }

    @Override
    public long getRunTime() {
      return Clock.lastKnownForwardProgressingMillis() + delayInMs;
    }

    @Override
    public void runTask() {
      runCalled = true;
    }

    @Override
    public short getExecuteReference() {
      return 0;
    }

    @Override
    public boolean canExecute(short executionReference) {
      canExecuteCalled = true;
      return true;
    }
  }
}
