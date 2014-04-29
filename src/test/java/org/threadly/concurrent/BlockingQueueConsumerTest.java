package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.BlockingQueueConsumer.ConsumerAcceptor;
import org.threadly.test.concurrent.TestCondition;

@SuppressWarnings("javadoc")
public class BlockingQueueConsumerTest {
   private SynchronousQueue<Object> queue;
   private TestAcceptor acceptor;
   private BlockingQueueConsumer<Object> queueConsumer;
   
  @Before
  public void setup() {
    queue = new SynchronousQueue<Object>();
    acceptor = new TestAcceptor();
    queueConsumer = new BlockingQueueConsumer<Object>(queue, acceptor);
  }
  
  @After
  public void tearDown() {
    queueConsumer.stop();
    queue = null;
    acceptor = null;
    queueConsumer = null;
  }
  
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
  public void doubleStartTest() {
    // start queue
    queueConsumer.maybeStart(Executors.defaultThreadFactory());
    
    assertTrue(queueConsumer.isRunning());
    
    // attempt to start again
    queueConsumer.maybeStart(Executors.defaultThreadFactory());
    // should still be running without exception
    assertTrue(queueConsumer.isRunning());
  }
  
  @Test
  public void doubleStopTest() {
    queueConsumer.maybeStart(Executors.defaultThreadFactory());
    assertTrue(queueConsumer.isRunning());
    
    queueConsumer.stop();
    assertFalse(queueConsumer.isRunning());
    
    queueConsumer.stop();
    assertFalse(queueConsumer.isRunning());
  }
  
  @Test
  public void consumeTest() throws InterruptedException {
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
