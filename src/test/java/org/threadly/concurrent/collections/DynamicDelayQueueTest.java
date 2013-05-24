package org.threadly.concurrent.collections;

import static org.junit.Assert.*;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.collections.DynamicDelayQueue;
import org.threadly.concurrent.collections.DynamicDelayQueue.ConsumerIterator;

@SuppressWarnings("javadoc")
public class DynamicDelayQueueTest {
  private static final int TEST_QTY = 10;
  
  private DynamicDelayQueue<TestDelayed> testQueue;
  
  @Before
  public void setup() {
    testQueue = new DynamicDelayQueue<TestDelayed>();
  }
  
  @After
  public void teardown() {
    testQueue = null;
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
  
  @Test
  public void sizeTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      testQueue.add(new TestDelayed(i));
      assertEquals(i+1, testQueue.size());
    }
  }
  
  @Test
  public void isEmptyTest() throws InterruptedException {
    assertTrue(testQueue.isEmpty());
    testQueue.add(new TestDelayed(0));
    assertFalse(testQueue.isEmpty());

    testQueue.take();

    assertTrue(testQueue.isEmpty());
  }
  
  @Test
  public void containsTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      testQueue.add(new TestDelayed(i));
    }
    for (int i = 0; i < TEST_QTY; i++) {
      TestDelayed td = new TestDelayed(i);
      assertTrue(testQueue.contains(td));
    }
    for (int i = TEST_QTY + 1; i < TEST_QTY * 2; i++) {
      TestDelayed td = new TestDelayed(i);
      assertFalse(testQueue.contains(td));
    }
  }
  
  @Test
  public void containsAllTest() {
    List<TestDelayed> comparisionList = new ArrayList<TestDelayed>(TEST_QTY + 1);
    assertTrue(testQueue.containsAll(comparisionList));
    assertTrue(testQueue.containsAll(comparisionList));
    comparisionList.add(new TestDelayed(Integer.MIN_VALUE));
    assertFalse(testQueue.containsAll(comparisionList));
    testQueue.add(new TestDelayed(Integer.MIN_VALUE));
    assertTrue(testQueue.containsAll(comparisionList));
    for (int i = 0; i < TEST_QTY; i++) {
      TestDelayed td = new TestDelayed(i);
      comparisionList.add(td);
      assertFalse(testQueue.containsAll(comparisionList));
      testQueue.add(td);
      assertTrue(testQueue.containsAll(comparisionList));
    }
    
    testQueue.add(new TestDelayed(Integer.MAX_VALUE));
    assertTrue(testQueue.containsAll(comparisionList));
  }
  
  @Test
  public void iteratorTest() {
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
  
  @Test
  public void consumerIteratorTest() throws InterruptedException {
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
   
  @Test
  public void sortTest() {
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
  
  @Test
  public void clearTest() {
    populatePositive(testQueue);
    assertEquals(TEST_QTY, testQueue.size());
    testQueue.clear();
    assertEquals(0, testQueue.size());
  }
     
  @Test
  public void peekTest() {
    TestDelayed item = new TestDelayed(100);
    testQueue.add(item);
    assertNull(testQueue.peek()); // assert peek in future is null
    
    for (int i = 0; i < TEST_QTY; i++) {
      item = new TestDelayed(i * - 1);
      testQueue.add(item);
      assertEquals(item, testQueue.peek());
    }
  }
  
  @Test
  public void pollTest() {
    populateRandom(testQueue);
    
    TestDelayed prev = testQueue.poll();
    for (int i = 0; i > TEST_QTY; i++) {
      assertTrue(prev.compareTo(testQueue.peek()) >= 0);
      
      prev = testQueue.poll();
    }
  }
  
  @Test
  public void removeAllTest() {
    List<TestDelayed> toRemoveList = new ArrayList<TestDelayed>(TEST_QTY);
    List<TestDelayed> comparisonList = new ArrayList<TestDelayed>(TEST_QTY);
    boolean flip = false;
    for (int i = 0; i < TEST_QTY; i++) {
      TestDelayed item = new TestDelayed(i);
      testQueue.add(item);
      comparisonList.add(item);
      if (flip) {
        toRemoveList.add(item);
        flip = false;
      } else {
        flip = true;
      }
    }
    
    testQueue.removeAll(toRemoveList);
    assertEquals(testQueue.size(), TEST_QTY - toRemoveList.size());
    Iterator<TestDelayed> it = toRemoveList.iterator();
    while (it.hasNext()) {
      assertFalse(testQueue.contains(it.next()));
    }
    
    comparisonList.removeAll(toRemoveList);  // do operation on comparison list
    assertTrue(testQueue.containsAll(comparisonList));  // verify nothing additional was removed
  }
  
  @Test (expected = NoSuchElementException.class)
  public void removeFail() {
    testQueue.remove();
    
    fail("Exception should have been thrown");
  }
  
  @Test (expected = NoSuchElementException.class)
  public void iteratorNextFail() {
    populatePositive(testQueue);
    synchronized (testQueue.getLock()) {
      Iterator<TestDelayed> it = testQueue.iterator();
      for(int i = 0; i <= TEST_QTY; i++) {
        it.next();
      }
    }
    
    fail("Exception should have been thrown");
  }
   
   @Test (expected = IllegalStateException.class)
   public void iteratorRemoveFail() {
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
   
   @Test (expected = NoSuchElementException.class)
   public void consumerIteratorFail() throws InterruptedException {
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
   
  @Test
  public void remainingCapacityTest() {
    assertEquals(testQueue.remainingCapacity(), Integer.MAX_VALUE);
  }
  
  @Test
  public void toArrayTest() {
    assertArrayEquals(testQueue.toArray(), new TestDelayed[0]);
    
    TestDelayed[] compare = new TestDelayed[TEST_QTY];
    for (int i = 0; i < TEST_QTY; i++) {
      TestDelayed td = new TestDelayed(i);
      compare[i] = td;
      testQueue.add(td);
    }
    
    assertArrayEquals(testQueue.toArray(), compare);
  }
  
  /*@Test
  public void drainToTest() {
    // TODO - implement
  }
  
  @Test
  public void drainToLimitTest() {
    // TODO - implement
  }*/
  
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
    
    @Override
    public boolean equals(Object o) {
      if (o instanceof TestDelayed) {
        TestDelayed td = (TestDelayed)o;
        return this.toString().equals(td.toString());
      } else {
        return false;
      }
    }
  }
}
