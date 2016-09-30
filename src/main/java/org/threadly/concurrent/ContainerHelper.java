package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * <p>Class which is designed to help with determining if a Runnable or Callable is contained at some 
 * point within a chain of {@link CallableContainer} or {@link RunnableContainer}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class ContainerHelper {
  /**
   * Attempts to remove the provided runnable from the source collection.  This uses the 
   * {@link Collection#remove(Object)} function to remove the container if it is found.
   * 
   * @since 2.0.0
   * 
   * @param source source collection to search over
   * @param compareTo Runnable to compare against in search
   * @return {@code true} if collection was modified
   */
  public static boolean remove(Collection<? extends RunnableContainer> source, 
                               Runnable compareTo) {
    Iterator<? extends RunnableContainer> it = source.iterator();
    while (it.hasNext()) {
      RunnableContainer rc = it.next();
      // we use source.remove() instead of it.remove() for usage with concurrent structures
      if (ContainerHelper.isContained(rc.getContainedRunnable(), compareTo) && source.remove(rc)) {
        return true;
      }
    }
    
    return false;
  }

  /**
   * Attempts to remove the provided callable from the source collection.  This uses the 
   * {@link Collection#remove(Object)} function to remove the container if it is found.
   * 
   * @since 2.0.0
   * 
   * @param source source collection to search over
   * @param compareTo Callable to compare against in search
   * @return {@code true} if collection was modified
   */
  public static boolean remove(Collection<? extends RunnableContainer> source, 
                               Callable<?> compareTo) {
    Iterator<? extends RunnableContainer> it = source.iterator();
    while (it.hasNext()) {
      RunnableContainer rc = it.next();
      // we use source.remove() instead of it.remove() for usage with concurrent structures
      if (ContainerHelper.isContained(rc.getContainedRunnable(), compareTo) && source.remove(rc)) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * Checks if the start runnable equals the compareTo runnable, or if the compareTo runnable is 
   * contained within a wrapper of the startRunnable.
   * 
   * @param startRunnable runnable to start search at
   * @param compareTo runnable to be comparing against
   * @return {@code true} if they are equivalent, or the compareTo runnable is contained within the start
   */
  public static boolean isContained(Runnable startRunnable, Runnable compareTo) {
    if (startRunnable.equals(compareTo)) {
      return true;
    }
    
    boolean containedAsRCCI = false;
    if (startRunnable instanceof RunnableContainer) {
      RunnableContainer rci = (RunnableContainer)startRunnable;
      containedAsRCCI = isContained(rci, compareTo);
    }
    if (containedAsRCCI) {
      return true;
    } else if (startRunnable instanceof CallableContainer<?>) {
      CallableContainer<?> cci = (CallableContainer<?>)startRunnable;
      return isContained(cci, compareTo);
    } else {
      return false;
    }
  }

  /**
   * Checks if the compareTo runnable is contained by the provided 
   * {@link RunnableContainerInterface}.  If it's not we check to see if we can continue our 
   * search by looking for another {@link RunnableContainer}, or a {@link CallableContainer}.
   * 
   * @param rci Container to check contents of
   * @param compareTo Runnable to compare against
   * @return {@code true} if the runnable is contained at some point within the container
   */
  private static boolean isContained(RunnableContainer rci, Runnable compareTo) {
    while (true) {
      Runnable containedTask = rci.getContainedRunnable();
      if (containedTask != null) {
        if (containedTask.equals(compareTo)) {
          return true;
        } else if (containedTask instanceof CallableContainer<?> && 
                   isContained((CallableContainer<?>)containedTask, compareTo)) {
          return true;
        } else if (containedTask instanceof RunnableContainer) {
          // loop again
          rci = (RunnableContainer)containedTask;
        } else {
          return false;
        }
      } else {
        return false;
      }
    }
  }

  /**
   * Checks if the compareTo runnable is contained by the provided 
   * {@link CallableContainerInterface}.  If it's not we check to see if we can continue our 
   * search by looking for another {@link RunnableContainer}, or a {@link CallableContainer}.
   * 
   * @param cci Container to check contents of
   * @param compareTo Runnable to compare against
   * @return {@code true} if the runnable is contained at some point within the container
   */
  private static boolean isContained(CallableContainer<?> cci, Runnable compareTo) {
    while (true) {
      Callable<?> containedTask = cci.getContainedCallable();
      if (containedTask != null) {
        if (containedTask instanceof RunnableContainer && 
            isContained((RunnableContainer)containedTask, compareTo)) {
          return true;
        } else if (containedTask instanceof CallableContainer<?>) {
          // loop again
          cci = (CallableContainer<?>)containedTask;
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
   * @return {@code true} if the compareTo runnable is contained within the runnable
   */
  public static boolean isContained(Runnable startRunnable, Callable<?> compareTo) {
    /* we have some awkward if/else logic in case we have a runnable the is both a 
     * CallableContainerInterface and a RunnableContainerInterface
     */
    boolean isContainedAsCCI = false;
    if (startRunnable instanceof CallableContainer<?>) {
      CallableContainer<?> cci = (CallableContainer<?>)startRunnable;
      isContainedAsCCI = isContained(cci, compareTo);
    }
    if (isContainedAsCCI) {
      return true;
    } else if (startRunnable instanceof RunnableContainer) {
      RunnableContainer rci = (RunnableContainer)startRunnable;
      return isContained(rci, compareTo);
    } else {
      return false;
    }
  }

  private static boolean isContained(RunnableContainer rci, Callable<?> compareTo) {
    while (true) {
      Runnable containedTask = rci.getContainedRunnable();
      if (containedTask != null) {
        if (containedTask instanceof CallableContainer<?> && 
            isContained((CallableContainer<?>)containedTask, compareTo)) {
          return true;
        } else if (containedTask instanceof RunnableContainer) {
          // loop again
          rci = (RunnableContainer)containedTask;
        } else {
          return false;
        }
      } else {
        return false;
      }
    }
  }

  /**
   * Checks if the compareTo runnable is contained by the provided 
   * {@link CallableContainerInterface}.  If it's not we check to see if we can continue our 
   * search by looking for another {@link RunnableContainer}, or a {@link CallableContainer}.
   * 
   * @param cci Container to check contents of
   * @param compareTo Callable to compare against
   * @return {@code true} if the callable is contained at some point within the container
   */
  private static boolean isContained(CallableContainer<?> cci, Callable<?> compareTo) {
    while (true) {
      Callable<?> containedTask = cci.getContainedCallable();
      if (containedTask != null) {
        if (containedTask.equals(compareTo)) {
          return true;
        } else if (containedTask instanceof RunnableContainer && 
                   isContained((RunnableContainer)containedTask, compareTo)) {
          return true;
        } else if (containedTask instanceof CallableContainer<?>) {
          // loop again
          cci = (CallableContainer<?>)containedTask;
        } else {
          return false;
        }
      } else {
        return false;
      }
    }
  }
  
  /**
   * Takes in a list of runnable containers, and instead makes a list of the runnables which are 
   * contained in each item of the list.
   * 
   * @param sourceList List of runnable containers to get interior runnable from
   * @return A list of runnables which were contained in the source list
   * @since 4.0.0
   */
  public static List<Runnable> getContainedRunnables(List<? extends RunnableContainer> sourceList) {
    List<Runnable> result = new ArrayList<>(sourceList.size());
    Iterator<? extends RunnableContainer> it = sourceList.iterator();
    while (it.hasNext()) {
      result.add(it.next().getContainedRunnable());
    }
    
    return result;
  }
}
