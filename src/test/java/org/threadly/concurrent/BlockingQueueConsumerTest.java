package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

import org.junit.Test;
import org.threadly.concurrent.BlockingQueueConsumer.ConsumerAcceptor;
import org.threadly.test.concurrent.TestCondition;

@SuppressWarnings("javadoc")
public class BlockingQueueConsumerTest {
  @SuppressWarnings("unused")
  @Test
  public void constructorFail() {
    try {
      new BlockingQueueConsumer<Object>(new SynchronousQueue<Object>(), null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new BlockingQueueConsumer<Object>(null, new TestAcceptor());
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void consumeTest() throws InterruptedException {
    SynchronousQueue<Object> queue = new SynchronousQueue<Object>();
    TestAcceptor acceptor = new TestAcceptor();
    BlockingQueueConsumer<Object> queueConsumer = new BlockingQueueConsumer<Object>(queue, acceptor);
    
    assertFalse(queueConsumer.isRunning());
    
    // start queue
    queueConsumer.maybeStart(Executors.defaultThreadFactory());
    
    assertTrue(queueConsumer.isRunning());
    
    Object item = new Object();
    queue.put(item);
    
    acceptor.blockTillTrue(); // will throw exception if never got item
    
    assertTrue(acceptor.acceptedItems.get(0) == item);
  }
  
  private static class TestAcceptor extends TestCondition 
                                    implements ConsumerAcceptor<Object> {
    private final List<Object> acceptedItems = new LinkedList<Object>();
    
    @Override
    public void acceptConsumedItem(Object item) throws Exception {
      synchronized (this) {
        acceptedItems.add(item);
      }
    }

    @Override
    public boolean get() {
      synchronized (this) {
        return ! acceptedItems.isEmpty();
      }
    }
  }
}
