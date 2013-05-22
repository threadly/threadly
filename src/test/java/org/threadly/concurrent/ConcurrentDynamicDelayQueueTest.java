package org.threadly.concurrent;

import java.util.NoSuchElementException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.DynamicDelayQueueTest.TestDelayed;

@SuppressWarnings("javadoc")
public class ConcurrentDynamicDelayQueueTest {
  private DynamicDelayQueue<TestDelayed> testQueue;
  
  @Before
  public void setup() {
    testQueue = new ConcurrentDynamicDelayQueue<TestDelayed>();
  }
  
  @After
  public void teardown() {
    testQueue = null;
  }
  
  @Test
  public void testSize() {
    DynamicDelayQueueTest.testSize(testQueue);
  }
  
  @Test
  public void testIterator() {
    DynamicDelayQueueTest.testIterator(testQueue);
  }
  
  @Test
  public void testConsumerIterator() throws InterruptedException {
    DynamicDelayQueueTest.testConsumerIterator(testQueue);
  }
   
  @Test
  public void testSort() {
    DynamicDelayQueueTest.testSort(testQueue);
  }
  
  @Test
  public void testClear() {
    DynamicDelayQueueTest.testClear(testQueue);
  }
     
  @Test
  public void testPeek() {
    DynamicDelayQueueTest.testPeek(testQueue);
  }
  
  @Test
  public void testPoll() {
    DynamicDelayQueueTest.testPoll(testQueue);
  }
  
  @Test (expected = NoSuchElementException.class)
  public void testRemoveFail() {
    DynamicDelayQueueTest.testRemoveFail(testQueue);
  }
  
  @Test (expected = NoSuchElementException.class)
  public void testIteratorNextFail() {
    DynamicDelayQueueTest.testIteratorNextFail(testQueue);
  }
   
   @Test (expected = IllegalStateException.class)
   public void testIteratorRemoveFail() {
     DynamicDelayQueueTest.testIteratorRemoveFail(testQueue);
   }
   
   @Test (expected = NoSuchElementException.class)
   public void testConsumerIteratorFail() throws InterruptedException {
     DynamicDelayQueueTest.testConsumerIteratorFail(testQueue);
   }
}
