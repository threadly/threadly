package org.threadly.concurrent;

import java.util.concurrent.Callable;

/**
 * Interface to implement if any classes are containing a callable.  This 
 * interface must be implemented in order for the {@link PriorityScheduledExecutor} 
 * remove function to work correctly if that wrapper is ever provided to the 
 * thread pool.
 * 
 * @author jent - Mike Jensen
 * @param <T> Type for type of callable contained
 */
public interface CallableContainerInterface<T> {
  /**
   * Call to get the contained callable held within the wrapper.
   * 
   * @return callable contained within wrapper
   */
  public Callable<T> getContainedCallable();
}
