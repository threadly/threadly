package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.concurrent.lock.LockFactory;
import org.threadly.concurrent.lock.VirtualLock;

@SuppressWarnings("javadoc")
public class VirtualRunnableTest {
  
  @Test
  public void runTest() {
    TestLockFactory tlf = new TestLockFactory();
    TestRunnable tr = new TestRunnable();
    
    assertNull(tr.factory); // null before run
    tr.run(tlf);
    assertNull(tr.factory); // null after run
    
    assertTrue(tr.runCalled);
    assertEquals(tr.returnedLock, tlf);
    assertEquals(tr.factoryAtRunTime, tlf);
    
    assertEquals(tlf.makeLockCallCount, 2); // 2 calls, one for lock request, one for sleep
    assertEquals(tlf.sleepCount, 1);
  }
  
  @Test
  public void makeWithoutFactoryTest() {
    TestRunnable tr = new TestRunnable();
    VirtualLock vl = tr.makeLock();
    assertNotNull(vl);
    assertTrue(tr.makeLock() != vl);
  }
  
  private class TestRunnable extends VirtualRunnable {
    private boolean runCalled = false;
    private LockFactory factoryAtRunTime = null;
    private VirtualLock returnedLock;

    @Override
    public void run() {
      factoryAtRunTime = factory;
      runCalled = true;
      returnedLock = makeLock();
      try {
        sleep(10);
      } catch (InterruptedException e) {
        // impossible
      }
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
