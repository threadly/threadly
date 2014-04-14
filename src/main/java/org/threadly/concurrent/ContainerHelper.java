package org.threadly.concurrent;

import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * Class which is designed to help with determining if a Runnable or Callable is contained 
 * at some point within a chain of {@link CallableContainerInterface} or {@link RunnableContainerInterface}.
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class ContainerHelper {
  /**
   * Attempts to remove the provided runnable from the source collection.  This 
   * uses the iterator .remove() function to remove the container if it is found.
   * 
   * @since 2.0.0
   * 
   * @param source source collection to search over
   * @param compareTo Runnable to compare against in search
   * @return true if collection was modified
   */
  public static boolean remove(Iterable<? extends RunnableContainerInterface> source, 
                               Runnable compareTo) {
    Iterator<? extends RunnableContainerInterface> it = source.iterator();
    while (it.hasNext()) {
      RunnableContainerInterface rc = it.next();
      if (ContainerHelper.isContained(rc.getContainedRunnable(), compareTo)) {
        it.remove();
        return true;
      }
    }
    
    return false;
  }

  /**
   * Attempts to remove the provided callable from the source collection.  This 
   * uses the iterator .remove() function to remove the container if it is found.
   * 
   * @since 2.0.0
   * 
   * @param source source collection to search over
   * @param compareTo Callable to compare against in search
   * @return true if collection was modified
   */
  public static boolean remove(Iterable<? extends RunnableContainerInterface> source, 
                               Callable<?> compareTo) {
    Iterator<? extends RunnableContainerInterface> it = source.iterator();
    while (it.hasNext()) {
      RunnableContainerInterface rc = it.next();
      if (ContainerHelper.isContained(rc.getContainedRunnable(), compareTo)) {
        it.remove();
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * Checks if the start runnable equals the compareTo runnable, or if 
   * the compareTo runnable is contained within a wrapper of the startRunnable.
   * 
   * @param startRunnable runnable to start search at
   * @param compareTo runnable to be comparing against
   * @return true if they are equivalent, or the compareTo runnable is contained within the start
   */
  public static boolean isContained(Runnable startRunnable, Runnable compareTo) {
    if (startRunnable.equals(compareTo)) {
      return true;
    }
    
    boolean containedAsRCCI = false;
    if (startRunnable instanceof RunnableContainerInterface) {
      RunnableContainerInterface rci = (RunnableContainerInterface)startRunnable;
      containedAsRCCI = isContained(rci, compareTo);
    }
    if (containedAsRCCI) {
      return true;
    } else if (startRunnable instanceof CallableContainerInterface<?>) {
      CallableContainerInterface<?> cci = (CallableContainerInterface<?>)startRunnable;
      return isContained(cci, compareTo);
    } else {
      return false;
    }
  }

  private static boolean isContained(RunnableContainerInterface rci, Runnable compareTo) {
    while (true) {
      Runnable containedTask = rci.getContainedRunnable();
      if (containedTask != null) {
        if (containedTask.equals(compareTo)) {
          return true;
        } else if (containedTask instanceof CallableContainerInterface<?> && 
                   isContained((CallableContainerInterface<?>)containedTask, compareTo)) {
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
  }

  private static boolean isContained(CallableContainerInterface<?> cci, Runnable compareTo) {
    while (true) {
      Callable<?> containedTask = cci.getContainedCallable();
      if (containedTask != null) {
        if (containedTask instanceof RunnableContainerInterface && 
            isContained((RunnableContainerInterface)containedTask, compareTo)) {
          return true;
        } else if (containedTask instanceof CallableContainerInterface<?>) {
          // loop again
          cci = (CallableContainerInterface<?>)containedTask;
        } else {
          return false;
        }
      } else {
        return false;
      }
    }
  }

  /**
   * Checks if the start runnable contains the provided callable.
   * 
   * @param startRunnable runnable to start search at
   * @param compareTo callable to be comparing against
   * @return true if the compareTo runnable is contained within the runnable
   */
  public static boolean isContained(Runnable startRunnable, Callable<?> compareTo) {
    /* we have some awkward if/else logic in case we have a runnable the is both a 
     * CallableContainerInterface and a RunnableContainerInterface
     */
    boolean isContainedAsCCI = false;
    if (startRunnable instanceof CallableContainerInterface<?>) {
      CallableContainerInterface<?> cci = (CallableContainerInterface<?>)startRunnable;
      isContainedAsCCI = isContained(cci, compareTo);
    }
    if (isContainedAsCCI) {
      return true;
    } else if (startRunnable instanceof RunnableContainerInterface) {
      RunnableContainerInterface rci = (RunnableContainerInterface)startRunnable;
      return isContained(rci, compareTo);
    } else {
      return false;
    }
  }

  private static boolean isContained(RunnableContainerInterface rci, Callable<?> compareTo) {
    while (true) {
      Runnable containedTask = rci.getContainedRunnable();
      if (containedTask != null) {
        if (containedTask instanceof CallableContainerInterface<?> && 
            isContained((CallableContainerInterface<?>)containedTask, compareTo)) {
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
  }

  private static boolean isContained(CallableContainerInterface<?> cci, Callable<?> compareTo) {
    while (true) {
      Callable<?> containedTask = cci.getContainedCallable();
      if (containedTask != null) {
        if (containedTask.equals(compareTo)) {
          return true;
        } else if (containedTask instanceof RunnableContainerInterface && 
                   isContained((RunnableContainerInterface)containedTask, compareTo)) {
          return true;
        } else if (containedTask instanceof CallableContainerInterface<?>) {
          // loop again
          cci = (CallableContainerInterface<?>)containedTask;
        } else {
          return false;
        }
      } else {
        return false;
      }
    }
  }
}
