package org.threadly.concurrent;

import java.security.SecureRandom;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.DynamicDelayQueue.ConsumerIterator;

public class SynchronizedDynamicDelayQueueTest {
  private static final int TEST_QTY = 100;
  
  private DynamicDelayQueue<TestDelayed> testQueue;
  
  @Before
  public void setup() {
    testQueue = new SynchronizedDynamicDelayQueue<TestDelayed>();
  }
  
  @After
  public void teardown() {
    testQueue = null;
  }
  
  @Test
  public void testSize() {
    for (int i = 0; i < TEST_QTY; i++) {
      testQueue.add(new TestDelayed(i));
      assertEquals(i+1, testQueue.size());
    }
  }
  
  private void populatePositive() {
    for (int i = 0; i < TEST_QTY; i++) {
      testQueue.add(new TestDelayed(i));
    }
  }
  
  private void populateNegative() {
    for (int i = TEST_QTY * -1; i < 0; i++) {
      testQueue.add(new TestDelayed(i));
    }
  }
  
  private void populateRandom() {
    Random random = new SecureRandom();
    for (int i = 0; i < TEST_QTY; i++) {
      testQueue.add(new TestDelayed(random.nextInt()));
    }
  }
  
  @Test
  public void testIterator() {
    synchronized (testQueue.getLock()) {
      populatePositive();
      
      int value = 0;
      Iterator<TestDelayed> it = testQueue.iterator();
      while (it.hasNext()) {
        assertEquals(it.next().getDelay(TimeUnit.MILLISECONDS), value++);
      }
      
      it = testQueue.iterator();
      for (int i = TEST_QTY; i > 0; i--) {
        assertEquals(i, testQueue.size());
        it.next();
        it.remove();
      }
    }
  }
  
  @Test
  public void testConsumerIterator() throws InterruptedException {
    synchronized (testQueue.getLock()) {
      populateNegative();
      
      int removed = 0;
      ConsumerIterator<TestDelayed> it = testQueue.consumeIterator();
      while (it.hasNext()) {
        TestDelayed peek = it.peek();
        assertEquals(it.remove(), peek);
        removed++;
        assertEquals(testQueue.size(), TEST_QTY - removed);
      }
      
      assertEquals(removed, TEST_QTY);
    }
  }
  
  private void verifyQueueOrder() {
    synchronized (testQueue.getLock()) {
      long lastDelay = Long.MIN_VALUE;
      Iterator<TestDelayed> it = testQueue.iterator();
      while (it.hasNext()) {
        TestDelayed td = it.next();
        long delay = td.getDelay(TimeUnit.MILLISECONDS);
        assertTrue(delay >= lastDelay);
        lastDelay = delay;
      }
    }
  }
   
  @Test
  public void testSort() {
    populateRandom();
    verifyQueueOrder();
    // add unsorted data
    Random random = new SecureRandom();
    for (int i = 0; i < TEST_QTY; i++) {
      testQueue.addLast(new TestDelayed(random.nextInt()));
    }
    
    testQueue.sortQueue();
    
    verifyQueueOrder();
  }
  
  @Test
  public void testClear() {
    populatePositive();
    assertEquals(TEST_QTY, testQueue.size());
    testQueue.clear();
    assertEquals(0, testQueue.size());
  }
     
  @Test
  public void testPeek(){
    TestDelayed item = new TestDelayed(100);
    testQueue.add(item);
    assertTrue(testQueue.peek() == null); // assert peek in future is null
    
    for (int i = 0; i < TEST_QTY; i++) {
      item = new TestDelayed(i * - 1);
      testQueue.add(item);
      assertEquals(item, testQueue.peek());
    }
  }
  
  @Test
  public void testPoll() {
   populateRandom();
         
   TestDelayed prev = testQueue.poll();
   for (int i = 0; i > TEST_QTY; i++) {
     assertTrue(prev.compareTo(testQueue.peek()) >= 0);
    
     prev = testQueue.poll();
    }
  }
  
  @Test (expected = NoSuchElementException.class)
  public void testRemoveFail() {
    testQueue.remove();
    fail("Exception should have been thrown");
  }
  
  @Test (expected = NoSuchElementException.class)
  public void testIteratorNextFail() {
    populatePositive();
    synchronized (testQueue.getLock()) {
      Iterator<TestDelayed> it = testQueue.iterator();
      for(int i = 0; i <= TEST_QTY; i++) {
        it.next();
      }
    }
    fail("Exception should have been thrown");
  }
   
   @Test (expected = IllegalStateException.class)
   public void testIteratorRemoveFail() {
     boolean thrown = false;
     Iterator<TestDelayed> it = testQueue.iterator();
     try {
       it.remove();
     } catch (IllegalStateException e) {
       thrown = true;
     }
     
     assertTrue(thrown);
     populatePositive();
     it = testQueue.iterator();
     for (int i = 0; i < testQueue.size() / 2; i++) {
       it.next();
     }
     
     it.remove();
     it.remove();
     
     fail("Exception should have been thrown");
   }
   
   @Test (expected = NoSuchElementException.class)
   public void testConsumerIteratorFail() throws InterruptedException {
     synchronized (testQueue.getLock()) {
       populateNegative();
       
       ConsumerIterator<TestDelayed> it = testQueue.consumeIterator();
       while (it.hasNext()) {
         it.remove();
       }
       
       it.remove();
       
       fail("Exception should have been thrown");
     }
   }
   
   private class TestDelayed implements Delayed {
     private final long delayInMs;
     
     private TestDelayed(long delayInMs) {
       this.delayInMs = delayInMs;
     }
     
     @Override
     public int compareTo(Delayed o) {
       if (this == o) {
         return 0;
       } else {
         long thisDelay = this.getDelay(TimeUnit.MILLISECONDS);
         long otherDelay = o.getDelay(TimeUnit.MILLISECONDS);
         if (thisDelay == otherDelay) {
           return 0;
         } else if (thisDelay > otherDelay) {
           return 1;
         } else {
           return -1;
         }
       }
     }
     
     @Override
     public long getDelay(TimeUnit unit) {
       return unit.convert(delayInMs, 
                           TimeUnit.MILLISECONDS);
     }
     
     @Override
     public String toString() {
       return "d:" + delayInMs;
     }
   }
}
