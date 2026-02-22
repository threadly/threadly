package org.threadly.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.SingleThreadScheduler.SchedulerManager;

@SuppressWarnings("javadoc")
public class SingleThreadSchedulerSchedulerManagerTest extends ThreadlyTester {
  @Test
  public void constructorTest() {
    SchedulerManager sm = new SchedulerManager(AbstractPriorityScheduler.DEFAULT_PRIORITY, 
                                               AbstractPriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, 
                                               new ConfigurableThreadFactory());
    
    assertNotNull(sm.scheduler);
    assertNotNull(sm.execThread);
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorFail() {
      assertThrows(IllegalThreadStateException.class, () -> {
      StartingThreadFactory threadFactory = new StartingThreadFactory();
      try {
        new SchedulerManager(AbstractPriorityScheduler.DEFAULT_PRIORITY, 
                             AbstractPriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, threadFactory);
      } finally {
        threadFactory.killThreads();
      }
      });
  }
}
