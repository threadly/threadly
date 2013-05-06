package org.threadly.concurrent;

import java.util.Queue;
import java.util.concurrent.Delayed;

public interface DynamicDelayQueue<T extends Delayed> extends Queue<T> {
  /**
   * Returns the object that will be called with .wait during .wait.  
   * And must be synchronized on while using the iterator.
   * @return object synchronized on internally
   */
  public Object getLock();
  
  /**
   * Does a full sort on the queue, this is usually not optimal.
   * It is better to call reposition(T e), but this could be used if 
   * many items moved at the same time.
   */
  public void sortQueue();
  
  /**
   * Called to reposition an item in the queue which's delay has updated
   * since original insertion (or was originally inserted as addLast().
   * 
   * @param e item currently in the queue
   */
  public void reposition(T e);
  
  /**
   * Adds an item to the end of the queue, used as an optimization from add(T e)
   * when it is known the item will be at the end of the queue
   * 
   * @param e item to add to queue
   */
  public void addLast(T e);
  
  /**
   * Takes the next item in the queue.  Call will block until next item delay is <= 0
   * 
   * @return next item ready to be consumed
   * @throws InterruptedException
   */
  public T take() throws InterruptedException;
  
  /**
   * Returns an iterator that consumes the queue as it is progressed.
   * 
   * @return ConsumerIterator for queue
   * @throws InterruptedException
   */
  public ConsumerIterator<T> consumeIterator() throws InterruptedException;
  
  public interface ConsumerIterator<E> {
    /**
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
     * Removes and returns the next available item in the queue
     * 
     * @return next item with delay <= 0
     */
    public E remove();
  }
}