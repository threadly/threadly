package org.threadly.concurrent.collections;

/**
 * <p>Iterator which automatically consumes the queue as it is iterated over.  The {@code next()} 
 * function has been replaced with {@link #remove()} to consume items as it iterates over the 
 * structure.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 * @param <E> Parameter for types of item to be returned by {@link #remove()} and {@link #peek()}
 */
public interface ConsumerIterator<E> {
  /**
   * Check if there are additional items to consume.
   * 
   * @return {@code true} if there is another item in the queue
   */
  public boolean hasNext();
  
  /**
   * Peek but don't remove the next item to be consumed, will return {@code null} if there is 
   * nothing left to consume in the queue.
   * 
   * @return next available item
   */
  public E peek();
  
  /**
   * Removes and returns the next available item in the queue.
   * 
   * @return next item with delay that is at least 0
   */
  public E remove();
}