package org.threadly.concurrent;

import java.util.concurrent.Callable;

/**
 * Class which is designed to help with determining if a Runnable or Callable is contained 
 * at some point within a chain of {@link CallableContainerInterface} or {@link RunnableContainerInterface}.
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
class ContainerHelper {
  /**
   * Checks if the start runnable equals the compareTo runnable, or if 
   * the compareTo runnable is contained within a wrapper of the startRunnable.
   * 
   * @param startRunnable runnable to start search at
   * @param compareTo runnable to be comparing against
   * @return true if they are equivalent, or the compareTo runnable is contained within the start
   */
  protected static boolean isContained(Runnable startRunnable, 
                                       Runnable compareTo) {
    if (startRunnable.equals(compareTo)) {
      return true;
    } else if (startRunnable instanceof RunnableContainerInterface) {
      // search if this is actually being wrapped by another object
      RunnableContainerInterface rci = (RunnableContainerInterface)startRunnable;
      while (true) {
        Runnable containedTask = rci.getContainedRunnable();
        if (containedTask != null) {
          if (containedTask.equals(compareTo)) {
            return true;
          } else if (containedTask instanceof RunnableContainerInterface) {
            // loop again
            rci = (RunnableContainerInterface)containedTask;
          } else {
            return false;
          }
        } else {
          return false;
        }
      }
    } else {
      return false;
    }
  }

  /**
   * Checks if the start runnable contains the provided callable.
   * 
   * @param startRunnable runnable to start search at
   * @param compareTo callable to be comparing against
   * @return true if the compareTo runnable is contained within the runnable
   */
  protected static boolean isContained(Runnable startRunnable, 
                                       Callable<?> compareTo) {
    if (startRunnable instanceof CallableContainerInterface<?>) {
      CallableContainerInterface<?> cci = (CallableContainerInterface<?>)startRunnable;
      while (true) {
        Callable<?> callable = cci.getContainedCallable();
        if (callable.equals(compareTo)) {
          return true;
        } else if (callable instanceof CallableContainerInterface<?>) {
          cci = (CallableContainerInterface<?>)callable;
        } else {
          return false;
        }
      }
    } else if (startRunnable instanceof RunnableContainerInterface) {
      RunnableContainerInterface rci = (RunnableContainerInterface)startRunnable;
      
      return isContained(rci.getContainedRunnable(), compareTo);
    } else {
      return false;
    }
  }
}
