package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.threadly.concurrent.PriorityScheduler.TaskWrapper;

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
  public void compareToTest() {
    TestWrapper tw0 = new TestWrapper(0);
    TestWrapper tw1 = new TestWrapper(1);
    TestWrapper tw10 = new TestWrapper(10);
    
    assertEquals(0, tw0.compareTo(tw0));
    assertEquals(-1, tw0.compareTo(tw1));
    assertEquals(1, tw10.compareTo(tw1));
    assertEquals(0, tw10.compareTo(new TestWrapper(10)));
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
    public long getDelay(TimeUnit unit) {
      return unit.convert(delayInMs, TimeUnit.MILLISECONDS);
    }
    
    @Override
    protected long getDelayInMs(long now) {
      return delayInMs;
    }

    @Override
    public void run() {
      runCalled = true;
    }

    @Override
    public boolean canExecute() {
      canExecuteCalled = true;
      return true;
    }
  }
}
