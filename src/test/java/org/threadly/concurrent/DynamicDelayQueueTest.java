package org.threadly.concurrent;

import java.security.SecureRandom;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

import org.threadly.concurrent.DynamicDelayQueue.ConsumerIterator;

@SuppressWarnings("javadoc")
public class DynamicDelayQueueTest {
  private static final int TEST_QTY = 10;
  
  public static void testSize(DynamicDelayQueue<TestDelayed> testQueue) {
    for (int i = 0; i < TEST_QTY; i++) {
      testQueue.add(new TestDelayed(i));
      assertEquals(i+1, testQueue.size());
    }
  }
  
  private static void populatePositive(DynamicDelayQueue<TestDelayed> testQueue) {
    for (int i = 0; i < TEST_QTY; i++) {
      testQueue.add(new TestDelayed(i));
    }
  }
  
  private static void populateNegative(DynamicDelayQueue<TestDelayed> testQueue) {
    for (int i = TEST_QTY * -1; i < 0; i++) {
      testQueue.add(new TestDelayed(i));
    }
  }
  
  private static void populateRandom(DynamicDelayQueue<TestDelayed> testQueue) {
    Random random = new SecureRandom();
    for (int i = 0; i < TEST_QTY; i++) {
      testQueue.add(new TestDelayed(random.nextInt()));
    }
  }
  
  public static void testIterator(DynamicDelayQueue<TestDelayed> testQueue) {
    synchronized (testQueue.getLock()) {
      populatePositive(testQueue);
      
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
  
  public static void testConsumerIterator(DynamicDelayQueue<TestDelayed> testQueue) throws InterruptedException {
    synchronized (testQueue.getLock()) {
      populateNegative(testQueue);
      
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
  
  private static void verifyQueueOrder(DynamicDelayQueue<TestDelayed> testQueue) {
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
  
  public static void testSort(DynamicDelayQueue<TestDelayed> testQueue) {
    populateRandom(testQueue);
    verifyQueueOrder(testQueue);
    // add unsorted data
    Random random = new SecureRandom();
    for (int i = 0; i < TEST_QTY; i++) {
      testQueue.addLast(new TestDelayed(random.nextInt()));
    }
    
    testQueue.sortQueue();
    
    verifyQueueOrder(testQueue);
  }
  
  public static void testClear(DynamicDelayQueue<TestDelayed> testQueue) {
    populatePositive(testQueue);
    assertEquals(TEST_QTY, testQueue.size());
    testQueue.clear();
    assertEquals(0, testQueue.size());
  }
  
  public static void testPeek(DynamicDelayQueue<TestDelayed> testQueue){
    TestDelayed item = new TestDelayed(100);
    testQueue.add(item);
    assertTrue(testQueue.peek() == null); // assert peek in future is null
    
    for (int i = 0; i < TEST_QTY; i++) {
      item = new TestDelayed(i * - 1);
      testQueue.add(item);
      assertEquals(item, testQueue.peek());
    }
  }
  
  public static void testPoll(DynamicDelayQueue<TestDelayed> testQueue) {
    populateRandom(testQueue);
    
    TestDelayed prev = testQueue.poll();
    for (int i = 0; i > TEST_QTY; i++) {
      assertTrue(prev.compareTo(testQueue.peek()) >= 0);
      
      prev = testQueue.poll();
    }
  }
  
  public static void testRemoveFail(DynamicDelayQueue<TestDelayed> testQueue) {
    testQueue.remove();
    fail("Exception should have been thrown");
  }
  
  public static void testIteratorNextFail(DynamicDelayQueue<TestDelayed> testQueue) {
    populatePositive(testQueue);
    synchronized (testQueue.getLock()) {
      Iterator<TestDelayed> it = testQueue.iterator();
      for(int i = 0; i <= TEST_QTY; i++) {
        it.next();
      }
    }
    fail("Exception should have been thrown");
  }
  
  public static void testIteratorRemoveFail(DynamicDelayQueue<TestDelayed> testQueue) {
    boolean thrown = false;
    Iterator<TestDelayed> it = testQueue.iterator();
    try {
      it.remove();
    } catch (IllegalStateException e) {
      thrown = true;
    }
    
    assertTrue(thrown);
    populatePositive(testQueue);
    it = testQueue.iterator();
    for (int i = 0; i < testQueue.size() / 2; i++) {
      it.next();
    }
    
    it.remove();
    it.remove();
    
    fail("Exception should have been thrown");
  }
  
  public static void testConsumerIteratorFail(DynamicDelayQueue<TestDelayed> testQueue) throws InterruptedException {
    synchronized (testQueue.getLock()) {
      populateNegative(testQueue);
      
      ConsumerIterator<TestDelayed> it = testQueue.consumeIterator();
      while (it.hasNext()) {
        it.remove();
      }
      
      it.remove();
      
      fail("Exception should have been thrown");
    }
  }
  
  protected static class TestDelayed implements Delayed {
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
