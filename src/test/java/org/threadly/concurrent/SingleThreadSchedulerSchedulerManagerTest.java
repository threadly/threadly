package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.concurrent.SingleThreadScheduler.SchedulerManager;

@SuppressWarnings("javadoc")
public class SingleThreadSchedulerSchedulerManagerTest {
  @Test
  public void constructorTest() {
    SchedulerManager sm = new SchedulerManager(AbstractPriorityScheduler.DEFAULT_PRIORITY, 
                                               AbstractPriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, 
                                               new ConfigurableThreadFactory());
    
    assertNotNull(sm.scheduler);
    assertNotNull(sm.execThread);
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalThreadStateException.class)
  public void constructorFail() {
    StartingThreadFactory threadFactory = new StartingThreadFactory();
    try {
      new SchedulerManager(AbstractPriorityScheduler.DEFAULT_PRIORITY, 
                           AbstractPriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, threadFactory);
    } finally {
      threadFactory.killThreads();
    }
  }
}
