package org.threadly.concurrent;

import java.util.concurrent.Callable;

/**
 * Interface to implement if any classes are containing a callable.  This interface must be 
 * implemented in order for the {@link PriorityScheduler} (and others) remove function to work 
 * correctly if that wrapper is ever provided to the thread pool.
 * 
 * @since 4.3.0 (since 1.0.0 as CallableContainerInterface)
 * @param <T> Type for type of callable contained
 */
public interface CallableContainer<T> {
  /**
   * Call to get the contained callable held within the wrapper.
   * 
   * @return callable contained within wrapper, or {@code null} if none is contained
   */
  public Callable<T> getContainedCallable();
}
