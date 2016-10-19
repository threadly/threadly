package org.threadly.concurrent;

/**
 * Interface to implement if any classes are containing a runnable.  This interface must be 
 * implemented in order for the {@link PriorityScheduler} (and others) remove function to work 
 * correctly if that wrapper is ever provided to the thread pool.
 * 
 * @since 4.3.0 (since 1.0.0 as RunnableContainerInterface)
 */
public interface RunnableContainer {
  /**
   * Call to get the contained runnable within the wrapper.
   * 
   * @return runnable contained, or {@code null} if none is contained
   */
  public Runnable getContainedRunnable();
}
