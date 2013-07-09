package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.threadly.concurrent.PriorityScheduledExecutor.TaskType;
import org.threadly.concurrent.PriorityScheduledExecutor.TaskWrapper;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class PriorityScheduledExecutorTaskWrapperTest {
  @Test
  public void cancelTest() {
    TestWrapper tw = new TestWrapper();
    assertFalse(tw.canceled);
    
    tw.cancel();
    
    assertTrue(tw.canceled);
  }
  
  @Test
  public void compareToTest() {
    TestWrapper tw0 = new TestWrapper(0);
    TestWrapper tw1 = new TestWrapper(1);
    TestWrapper tw10 = new TestWrapper(10);
    
    assertEquals(tw0.compareTo(tw0), 0);
    assertEquals(tw0.compareTo(tw1), -1);
    assertEquals(tw10.compareTo(tw1), 1);
    assertEquals(tw10.compareTo(new TestWrapper(10)), 0);
  }
  
  private class TestWrapper extends TaskWrapper {
    private final int delayInMs;
    @SuppressWarnings("unused")
    private boolean executingCalled;
    @SuppressWarnings("unused")
    private boolean runCalled;
    
    protected TestWrapper() {
      this(0);
    }
    
    protected TestWrapper(int delay) {
      this(TaskType.OneTime, 
           new TestRunnable(), 
           TaskPriority.High, delay);
    }
    
    protected TestWrapper(TaskType taskType, 
                          Runnable task,
                          TaskPriority priority, 
                          int delayInMs) {
      super(taskType, task, priority);
      
      this.delayInMs = delayInMs;
      executingCalled = false;
      runCalled = false;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return TimeUnit.MILLISECONDS.convert(delayInMs, unit);
    }

    @Override
    public void run() {
      runCalled = true;
    }

    @Override
    public void executing() {
      executingCalled = true;
    }
    
  }
}
