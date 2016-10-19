package org.threadly.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.util.AbstractService;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionUtils;

/**
 * Producer consumer problems are very frequent within multi-threaded code.  This class is 
 * designed to be a throttle for both sides of the problem.  It takes in a {@link BlockingQueue} 
 * so that items are only consumed as they become available.  At the same time it has a 
 * {@link ConsumerAcceptor} that will only accept items as it is ready.  By accepting on the same 
 * thread as the consumer it will only try to take more items after the 
 * {@link ConsumerAcceptor#acceptConsumedItem(Object)} call returns.
 * <p>
 * Another way to look at it, this class provides the thread to handle blocking when consuming 
 * from a {@link BlockingQueue}.
 * <p>
 * Keep in mind that this class in no way attempts to solve the problem if the program is 
 * producing faster than the consumer accepts.  In those conditions the queue will still continue 
 * to grow, and consume memory.
 * 
 * @since 1.0.0
 * @param <T> Type of items contained in the queue to be consumed
 */
public class BlockingQueueConsumer<T> extends AbstractService {
  private static final AtomicInteger DEFAULT_CONSUMER_VALUE = new AtomicInteger(0);
  
  private static String getDefaultThreadName() {
    return "QueueConsumer-" + DEFAULT_CONSUMER_VALUE.getAndIncrement();
  }
  
  protected final ThreadFactory threadFactory;
  protected final String threadName;
  protected final BlockingQueue<? extends T> queue;
  protected final ConsumerAcceptor<? super T> acceptor;
  protected volatile Thread runningThread;
  
  /**
   * Constructs a new consumer, with a provided queue to consume from, and an acceptor to accept 
   * items.
   * 
   * @param threadFactory ThreadFactory to construct new thread for consumer to run on 
   * @param queue queue to consume from
   * @param acceptor acceptor to provide consumed items to
   */
  public BlockingQueueConsumer(ThreadFactory threadFactory, 
                               BlockingQueue<? extends T> queue, 
                               ConsumerAcceptor<? super T> acceptor) {
    this(threadFactory, null, queue, acceptor);
  }
  
  /**
   * Constructs a new consumer, with a provided queue to consume from, and an acceptor to accept 
   * items.
   * 
   * @param threadFactory ThreadFactory to construct new thread for consumer to run on 
   * @param threadName Name of thread consumer runs on, or {@code null} to generate a default one
   * @param queue queue to consume from
   * @param acceptor acceptor to provide consumed items to
   */
  public BlockingQueueConsumer(ThreadFactory threadFactory, 
                               String threadName, 
                               BlockingQueue<? extends T> queue, 
                               ConsumerAcceptor<? super T> acceptor) {
    ArgumentVerifier.assertNotNull(threadFactory, "threadFactory");
    ArgumentVerifier.assertNotNull(queue, "queue");
    ArgumentVerifier.assertNotNull(acceptor, "acceptor");
    
    this.threadFactory = threadFactory;
    this.threadName = threadName;
    this.queue = queue;
    this.acceptor = acceptor;
    runningThread = null;
  }

  @Override
  protected void startupService() {
    runningThread = threadFactory.newThread(new ConsumerRunnable());
    if (runningThread.isAlive()) {
      throw new IllegalThreadStateException();
    }
    runningThread.setDaemon(true);
    if (threadName == null || threadName.isEmpty()) {
      runningThread.setName(getDefaultThreadName());
    } else {
      runningThread.setName(threadName);
    }
    runningThread.start();
  }

  @Override
  protected void shutdownService() {
    Thread runningThread = this.runningThread;
    this.runningThread = null;
    runningThread.interrupt();
  }
  
  /**
   * This function is provided so that it can be Overridden if necessary.  One example would be if 
   * any locking needs to happen while consuming.
   * 
   * @return the next consumed item
   * @throws InterruptedException thrown if thread is interrupted while blocking for next item
   */
  protected T getNext() throws InterruptedException {
    return queue.take();
  }
  
  /**
   * Class which represents our runnable actions for the consumer.
   *  
   * @since 1.0.0
   */
  private class ConsumerRunnable implements Runnable {
    @Override
    public void run() {
      while (runningThread != null) {
        try {
          T next = getNext();
          
          acceptor.acceptConsumedItem(next);
        } catch (InterruptedException e) {
          stopIfRunning();
          // reset interrupted status
          Thread.currentThread().interrupt();
        } catch (Throwable t) {
          ExceptionUtils.handleException(t);
        }
      }
    }
  }
  
  /**
   * Interface for an implementation which can accept consumed tasks.  You must provide an 
   * implementation of this interface on construction of the {@link BlockingQueueConsumer}.
   * 
   * @since 1.0.0
   * @param <T> Type of item this acceptor will receive
   */
  public interface ConsumerAcceptor<T> {
    /**
     * Called when ever the queue consumer has removed an item from the queue.  This call should 
     * block until the acceptor is ready for another item.
     * 
     * @param item Object that was removed from the queue
     * @throws Exception possible exception that could be thrown
     */
    public void acceptConsumedItem(T item) throws Exception;
  }
}
