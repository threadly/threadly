package org.threadly.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.StringUtils;

/**
 * Migration off this deprecated implementation is highly encouraged.  Javadocs removed in 5.37 
 * in replacement for {@link org.threadly.concurrent.processing.BlockingQueueConsumer}.  This class 
 * slipped under the radar, causing the implementation and interface to be significantly behind in 
 * threadly 5.X.  Please switch to 
 * {@link Poller#consumeQueue(java.util.Queue, java.util.function.Consumer)} or 
 * {@link org.threadly.concurrent.processing.BlockingQueueConsumer}.
 * 
 * @deprecated See {@link Poller#consumeQueue(java.util.Queue, java.util.function.Consumer)} or 
 *               {@link org.threadly.concurrent.processing.BlockingQueueConsumer} for replacement
 * 
 * @since 1.0.0
 * @param <T> Type of items contained in the queue to be consumed
 */
@Deprecated
public class BlockingQueueConsumer<T> extends org.threadly.concurrent.processing.BlockingQueueConsumer<T> {
  private static final AtomicInteger DEFAULT_CONSUMER_VALUE = new AtomicInteger(0);
  
  private static String getDefaultThreadName() {
    return "QueueConsumer-" + DEFAULT_CONSUMER_VALUE.getAndIncrement();
  }
  
  protected final String threadName;
  protected final ConsumerAcceptor<? super T> acceptor;
  
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
    super(threadFactory, queue);
    
    ArgumentVerifier.assertNotNull(acceptor, "acceptor");
    
    this.threadName = threadName;
    this.acceptor = acceptor;
  }

  @Override
  protected void startupService() {
    super.startupService();
    
    if (StringUtils.isNullOrEmpty(threadName)) {
      runningThread.setName(getDefaultThreadName());
    } else {
      runningThread.setName(threadName);
    }
  }

  @Override
  protected void accept(T next) throws Exception {
    acceptor.acceptConsumedItem(next);
  }

  @Override
  protected void handleException(Throwable t) {
    ExceptionUtils.handleException(t);
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
