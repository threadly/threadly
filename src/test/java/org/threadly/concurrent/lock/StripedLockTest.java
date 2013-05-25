package org.threadly.concurrent.lock;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("javadoc")
public class StripedLockTest {
  private static final int LOCK_QTY = 10;
  
  private StripedLock sLock;
  
  @Before
  public void setup() {
    sLock = new StripedLock(LOCK_QTY, new NativeLockFactory());
  }
  
  @After
  public void tearDown() {
    sLock = null;
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void constructorNegativeConcurrencyFail() {
    new StripedLock(-10, new NativeLockFactory());
    
    fail("Exception should have been thrown");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void constructorNullFactoryFail() {
    new StripedLock(0, null);
    
    fail("Exception should have been thrown");
  }
  
  @Test
  public void getLockObjectTest() {
    Object testKey1 = new Object();
    Object testKey2 = new Object();
    while (testKey2.hashCode() % LOCK_QTY == testKey1.hashCode() % LOCK_QTY) {
      testKey2 = new Object();  // verify they will get different locks
    }
    
    VirtualLock lock1 = sLock.getLock(testKey1);
    assertNotNull(lock1);
    VirtualLock lock2 = sLock.getLock(testKey2);
    
    assertTrue(lock1 != lock2);
    assertTrue(sLock.getLock(testKey1) == lock1);
    assertTrue(sLock.getLock(testKey2) == lock2);
  }
  
  @Test
  public void getLockNullTest() {
    assertTrue(sLock.getLock(0) == sLock.getLock(null));
  }
  
  @Test
  public void getLockHashCodeTest() {
    Object testKey1 = new Object();
    
    VirtualLock lock = sLock.getLock(testKey1.hashCode());
    assertNotNull(lock);
    
    assertTrue(sLock.getLock(testKey1) == lock);
    assertTrue(sLock.getLock(testKey1.hashCode() + LOCK_QTY) == lock);
  }
}
