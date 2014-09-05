package org.threadly.concurrent.collections;

/**
 * <p>Interface to be called from the queue when delayed item is ready to be updated.</p>
 * 
 * <p>This is used within threadly for recurring tasks to adjust their delay after execution.  
 * Threadly uses an idea of delay items which can adjust there delay time, but this must be done 
 * in conjunction with the DynamicDelayQueue.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public interface DynamicDelayedUpdater {
  /**
   * Call that will happen from the Queue when it is ready for the item to update or changes it's 
   * time.  The process is that a call to the queue with the item, and what it's desired new value 
   * is done.  When the queue is ready it will call to this function to allow the item to update 
   * its delay before returning from the reposition call.
   * 
   * This call is expected to be very light-weight and fast, as the queue
   * lock will be held while making the call.
   */
  public void allowDelayUpdate();
}
