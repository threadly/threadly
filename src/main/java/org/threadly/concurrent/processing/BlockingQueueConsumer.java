package org.threadly.concurrent.processing;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

import org.threadly.util.AbstractService;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionHandler;
import org.threadly.util.ExceptionUtils;

/**
 * This class is designed for handling the threading and consumption of a high throughput 
 * {@link BlockingQueue}.  This class will create a thread to block on for the queue, and provide 
 * the result to the abstract {@link #accept(Object)}.  For low throughput queues, or 
 * where you want threads shared, consider using instead 
 * {@link org.threadly.concurrent.Poller#consumeQueue(java.util.Queue, Consumer, ExceptionHandler)}.
 * <p>
 * This implementation will create a single thread, serially calling to {@link #accept(Object)}, 
 * and not attempting to consume the next item till after it returns.  If any errors happen similar 
 * {@link #handleException(Throwable)} will block consuming the next item from the queue.
 * <p>
 * By implementing {@link org.threadly.util.Service} this class handles the thread lifecycle.  Item 
 * consumption will start when {@link #start()} is invoked, and will continue until {@link #stop()} 
 * is invoked.  When stopped, the consumer thread will be interrupted.  This is necessary to unblock 
 * from consuming from the queue, but may impact the accepting code if it is currently handling an 
 * item.
 * 
 * @since 5.37 (Former version existed at org.threadly.concurrent)
 * @param <T> Type of items contained in the queue to be consumed
 */
public abstract class BlockingQueueConsumer<T> extends AbstractService {
  /**
   * Construct a new {@link BlockingQueueConsumer} in case an abstract implementation is not 
   * preferred.
   * 
   * @param threadFactory ThreadFactory to construct new thread for consumer to run on 
   * @param queue Queue to consume items from
   * @param consumer Consumer to provide items from queue on
   * @param exceptionHandler Handler to send errors to, if {@code null} {@link ExceptionUtils#handleException} is used
   * @return A new {@link BlockingQueueConsumer} ready to {@code start()}
   */
  public static <T> BlockingQueueConsumer<T> makeForHandlers(ThreadFactory threadFactory, 
                                                             BlockingQueue<? extends T> queue, 
                                                             Consumer<T> consumer, 
                                                             ExceptionHandler exceptionHandler) {
    ArgumentVerifier.assertNotNull(consumer, "consumer");
    final ExceptionHandler fExceptionHandler;
    if (exceptionHandler == null) {
      fExceptionHandler = ExceptionUtils::handleException;
    } else {
      fExceptionHandler = exceptionHandler;
    }
    
    return new BlockingQueueConsumer<T>(threadFactory, queue) {
      @Override
      protected void accept(T next) throws Exception {
        consumer.accept(next);
      }

      @Override
      protected void handleException(Throwable t) {
        fExceptionHandler.handleException(t);
      }
    };
  }
  
  protected final ThreadFactory threadFactory;
  protected final BlockingQueue<? extends T> queue;
  protected volatile Thread runningThread = null;
  
  /**
   * Constructs a new consumer, with a provided queue to consume from, and an acceptor to accept 
   * items.
   * 
   * @param threadFactory ThreadFactory to construct new thread for consumer to run on 
   * @param queue Queue to consume items from
   */
  public BlockingQueueConsumer(ThreadFactory threadFactory, BlockingQueue<? extends T> queue) {
    ArgumentVerifier.assertNotNull(threadFactory, "threadFactory");
    ArgumentVerifier.assertNotNull(queue, "queue");
    
    this.threadFactory = threadFactory;
    this.queue = queue;
  }

  @Override
  protected void startupService() {
    runningThread = threadFactory.newThread(new ConsumerRunnable());
    if (runningThread.isAlive()) {
      throw new IllegalThreadStateException();
    }
    runningThread.setDaemon(true);
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
  
  protected abstract void accept(T next) throws Exception;
  
  protected abstract void handleException(Throwable t);
  
  /**
   * Class which represents our runnable actions for the consumer.
   *  
   * @since 5.37
   */
  protected final class ConsumerRunnable implements Runnable {
    @Override
    public void run() {
      while (runningThread != null) {
        try {
          accept(getNext());
        } catch (InterruptedException e) {
          stopIfRunning();
        } catch (Throwable t) {
          handleException(t);
        }
      }
    }
  }
}
