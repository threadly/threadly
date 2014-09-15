package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.concurrent.SingleThreadScheduler.SchedulerManager;

@SuppressWarnings("javadoc")
public class SingleThreadSchedulerSchedulerManagerTest {
  @Test
  public void constructorTest() {
    SchedulerManager sm = new SchedulerManager(new ConfigurableThreadFactory());
    
    assertNotNull(sm.scheduler);
    assertNotNull(sm.execThread);
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalThreadStateException.class)
  public void constructorFail() {
    new SchedulerManager(new StartingThreadFactory());
  }
}
