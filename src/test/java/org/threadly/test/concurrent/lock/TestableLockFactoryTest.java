package org.threadly.test.concurrent.lock;

import static org.junit.Assert.*;

import java.util.concurrent.Executor;

import org.junit.Test;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.test.concurrent.TestablePriorityScheduler;

@SuppressWarnings("javadoc")
public class TestableLockFactoryTest {
  
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new TestableLockFactory(null);
    
    fail("Exception was supposed to be thrown");
  }
  
  @Test
  public void makeLockTest() {
    TestablePriorityScheduler tps = new TestablePriorityScheduler(new Executor() {
      @Override
      public void execute(Runnable command) {
        throw new UnsupportedOperationException("Not a real executor");
      }
    }, TaskPriority.High);
    
    TestableLockFactory tlf = new TestableLockFactory(tps);
    
    VirtualLock testResult = tlf.makeLock();
    
    assertTrue(testResult != null);
    assertTrue(testResult instanceof TestableLock);
  }
}
