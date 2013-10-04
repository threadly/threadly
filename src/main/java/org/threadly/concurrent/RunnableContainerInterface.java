package org.threadly.concurrent;

/**
 * Interface to implement if any classes are containing a runnable.  This 
 * interface must be implemented in order for the {@link PriorityScheduledExecutor} 
 * remove function to work correctly if that wrapper is ever provided to the 
 * thread pool.
 * 
 * @author jent - Mike Jensen
 */
public interface RunnableContainerInterface {
  /**
   * Call to get the contained runnable within the wrapper.
   * 
   * @return runnable contained, or null if none is contained
   */
  public Runnable getContainedRunnable();
}
