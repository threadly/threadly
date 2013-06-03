package org.threadly.concurrent.collections;

/**
 * Interface to be called from the queue when delayed item is ready to be updated.
 * 
 * @author jent - Mike Jensen
 */
public interface DynamicDelayedUpdater {
  /**
   * Call that will happen from the Queue when it is ready 
   * for the item to update or changes it's time.  The process is 
   * that a call to the queue with the item, and what it's desired
   * new value is done.  When the queue is ready it will call to this 
   * function to allow the item to update it's delay before returning 
   * from the reposition call.
   * 
   * This call is expected to be very light-weight and fast, as the queue
   * lock will be held while making the call.
   */
  public void allowDelayUpdate();
}
