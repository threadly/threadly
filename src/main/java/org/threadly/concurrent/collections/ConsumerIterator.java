package org.threadly.concurrent.collections;

/**
 * Iterator which automatically consumes the queue as it is iterated over.
 * 
 * @author jent - Mike Jensen
 * 
 * @param <E> Parameter for types of item to be returned by next() and peek()
 */
public interface ConsumerIterator<E> {
  /**
   * Check if there are additional items to consume.
   * 
   * @return true if there is another item in the queue
   */
  public boolean hasNext();
  
  /**
   * Peek but don't remove the next item to be consumed, will return null if
   * there is nothing left to consume in the queue.
   * 
   * @return next available item
   */
  public E peek();
  
  /**
   * Removes and returns the next available item in the queue.
   * 
   * @return next item with delay <= 0
   */
  public E remove();
}