package org.threadly.concurrent.lock;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class NativeLockTest {
  private static final NativeLock lock = new NativeLock();
  
  @Test
  public void awaitTimeoutTest() {
    final long waitTimeInMs = 10;
    
    long start;
    long resume;
    synchronized (lock) {
      start = System.currentTimeMillis();
      try {
        lock.await(waitTimeInMs);
      } catch (InterruptedException e) {
        fail("Exception: " + e);
      }
      resume = System.currentTimeMillis();
    }
    
    assertTrue(resume - start >= waitTimeInMs);
    assertTrue(resume - start < waitTimeInMs + 50);
  }
  
  @Test
  public void awaitUninterruptiblyTimeoutTest() {
    final long waitTimeInMs = 10;

    long start;
    long resume;
    synchronized (lock) {
      start = System.currentTimeMillis();
      lock.awaitUninterruptibly(waitTimeInMs);
      resume = System.currentTimeMillis();
    }
    
    assertTrue(resume - start >= waitTimeInMs);
    assertTrue(resume - start < waitTimeInMs + 50);
  }
  
  @Test
  public void sleepTest() {
    final long sleepTimeInMs = 10;
    
    long start = System.currentTimeMillis();
    try {
      lock.sleep(sleepTimeInMs);
    } catch (InterruptedException e) {
      fail("Exception: " + e);
    }
    long resume = System.currentTimeMillis();
    
    assertTrue(resume - start >= sleepTimeInMs);
    assertTrue(resume - start < sleepTimeInMs + 50);
  }
  
  @Test
  public void sleepUninterruptiblyTest() {
    final long sleepTimeInMs = 10;
    
    long start = System.currentTimeMillis();
    lock.sleepUninterruptibly(sleepTimeInMs);
    long resume = System.currentTimeMillis();
    
    assertTrue(resume - start >= sleepTimeInMs);
    assertTrue(resume - start < sleepTimeInMs + 50);
  }
}
