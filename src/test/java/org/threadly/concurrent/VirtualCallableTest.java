package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.concurrent.lock.LockFactory;
import org.threadly.concurrent.lock.VirtualLock;

@SuppressWarnings("javadoc")
public class VirtualCallableTest {
  
  @Test
  public void runTest() throws Exception {
    TestLockFactory tlf = new TestLockFactory();
    TestCallable tc = new TestCallable();
    
    assertNull(tc.factory); // null before run
    tc.call(tlf);
    assertNull(tc.factory); // null after run
    
    assertTrue(tc.runCalled);
    assertEquals(tc.returnedLock, tlf);
    assertEquals(tc.factoryAtRunTime, tlf);
    
    assertEquals(tlf.makeLockCallCount, 2); // 2 calls, one for lock request, one for sleep
    assertEquals(tlf.sleepCount, 1);
  }
  
  @Test
  public void makeWithoutFactoryTest() {
    TestCallable tc = new TestCallable();
    VirtualLock vl = tc.makeLock();
    assertNotNull(vl);
    assertTrue(tc.makeLock() != vl);
  }
  
  private class TestCallable extends VirtualCallable<Object> {
    private boolean runCalled = false;
    private LockFactory factoryAtRunTime = null;
    private VirtualLock returnedLock;

    @Override
    public Object call() {
      factoryAtRunTime = factory;
      runCalled = true;
      returnedLock = makeLock();
      try {
        sleep(10);
      } catch (InterruptedException e) {
        // impossible
      }
      
      return new Object();
    }
  }
  
  private class TestLockFactory extends VirtualLock implements LockFactory {
    private int makeLockCallCount = 0;
    private int sleepCount = 0;

    @Override
    public VirtualLock makeLock() {
      makeLockCallCount++;
      return this;
    }

    @Override
    public void await() throws InterruptedException {
      // ignored
    }

    @Override
    public void await(long waitTimeInMs) throws InterruptedException {
      // ignored
    }

    @Override
    public void signal() {
      // ignored
    }

    @Override
    public void signalAll() {
      // ignored
    }

    @Override
    public void sleep(long timeInMs) throws InterruptedException {
      sleepCount++;
    }

    @Override
    public boolean isNativeLockFactory() {
      return false;
    }
  }
}
