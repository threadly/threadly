package org.threadly.concurrent;

/**
 * <p>Interface to implement if any classes are containing a runnable.  This 
 * interface must be implemented in order for the {@link PriorityScheduler} 
 * (and others) remove function to work correctly if that wrapper is ever 
 * provided to the thread pool.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public interface RunnableContainerInterface {
  /**
   * Call to get the contained runnable within the wrapper.
   * 
   * @return runnable contained, or null if none is contained
   */
  public Runnable getContainedRunnable();
}
