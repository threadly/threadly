package org.threadly.concurrent;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;

/**
 * Producer consumer problems are very frequent within multi-threaded code.  This class 
 * is designed to be a throttle for both sides of the problem.  It takes in a BlockingQueue 
 * so that items are only consumed as they become available.  At the same time it has a 
 * {@link ConsumerAcceptor} that will only accept items as it is ready.  By accepting on 
 * the same thread as the consumer it will only try to take more items after the 
 * acceptConsumedItem call returns.
 * 
 * Another way to look at it, this class provides the thread to handle blocking when consuming 
 * from a BlockingQueue.
 * 
 * Keep in mind that this class in no way attempts to solve the problem if your producing on the 
 * queue faster than the consumer accepts.  In those conditions the queue will still continue to 
 * grow, and consume memory.
 * 
 * @author jent - Mike Jensen
 * @param <T> Type of items contained in the queue to be consumed
 */
public class BlockingQueueConsumer<T> implements Runnable {
  protected final BlockingQueue<T> queue;
  protected final ConsumerAcceptor<T> acceptor;
  protected volatile boolean started;
  protected volatile boolean stopped;
  protected volatile Thread runningThread;
  
  /**
   * Constructs a new consumer, with a provided queue to consume from, 
   * and an acceptor to accept items.
   * 
   * @param queue queue to consume from
   * @param acceptor acceptor to provide consumed items to
   */
  public BlockingQueueConsumer(BlockingQueue<T> queue,
                               ConsumerAcceptor<T> acceptor) {
    if (queue == null) {
      throw new IllegalArgumentException("Must provide a queue to consume from");
    } else if (acceptor == null) {
      throw new IllegalArgumentException("Must provide an acceptor to provide consumed items to");
    }
    
    this.queue = queue;
    this.acceptor = acceptor;
    started = false;
    stopped = false;
    runningThread = null;
  }

  /**
   * Getter to check if the consumer is currently running.
   * 
   * @return true if started and has not stopped yet.
   */
  public boolean isRunning() {
    return started && ! stopped;
  }
  
  /**
   * Will start the consumer if it is not already started.  This is 
   * designed so you can efficently call into this multiple times, and 
   * it will safely garuntee that this will only be started once.
   * 
   * @param threadFactory ThreadFactory to create new thread from
   * @param threadName Name to set the new thread to
   */
  public void maybeStart(ThreadFactory threadFactory, 
                         String threadName) {
    /* this looks like a double check but 
     * due to being volatile and only changing 
     * one direction should be safe, as well as the fact 
     * that started is a primitive (can't be half constructed)
     */
    if (started) {
      return;
    }
    
    synchronized (this) {
      if (started) {
        return;
      }

      started = true;
      runningThread = threadFactory.newThread(this);
      runningThread.setDaemon(true);
      runningThread.setName(threadName);
      runningThread.start();
    }
  }
  
  /**
   * Stops the thread which is consuming from the queue.  Once 
   * stopped this instance can no longer be started.  You must 
   * create a new instance.
   */
  public void stop() {
    /* this looks like a double check but 
     * due to being volatile and only changing 
     * one direction should be safe, as well as the fact 
     * that started and stopped are primitives
     */
    if (stopped || ! started) {
      return;
    }
    
    synchronized (this) {
      if (stopped || ! started) {
        return;
      }

      stopped = true;
      Thread runningThread = this.runningThread;
      this.runningThread = null;
      runningThread.interrupt();
    }
  }
  
  /**
   * This function is provided so that it can be Overridden if necessary.  
   * One example would be if any locking needs to happen while consuming.
   * 
   * @return the next consumed item
   * @throws InterruptedException thrown if thread is interrupted while blocking for next item
   */
  protected T getNext() throws InterruptedException {
    return queue.take();
  }
  
  @Override
  public void run() {
    while (! stopped) {
      try {
        T next = getNext();
        
        acceptor.acceptConsumedItem(next);
      } catch (InterruptedException e) {
        stop();
        Thread.currentThread().interrupt();
      } catch (Throwable t) {
        UncaughtExceptionHandler handler = Thread.getDefaultUncaughtExceptionHandler();
        if (handler != null) {
          handler.uncaughtException(Thread.currentThread(), t);
        } else {
          t.printStackTrace();
        }
      }
    }
  }
  
  /**
   * Interface for an implementation which can accept
   * consumed tasks.
   * 
   * @author jent - Mike Jensen
   * @param <T> Type of item this acceptor will receive
   */
  public interface ConsumerAcceptor<T> {
    /**
     * Called when ever the queue consumer has removed an item from the queue.  
     * This call should block until the acceptor is ready for another item.
     * 
     * @param item Object that was removed from the queue
     * @throws Exception possible exception that could be thrown
     */
    public void acceptConsumedItem(T item) throws Exception;
  }
}
