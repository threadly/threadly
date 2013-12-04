package org.threadly.concurrent.future;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * <p>A collection of small utilities for handling futures.</p>
 * 
 * @author jent - Mike Jensen
 */
public class FutureUtils {
  /**
   * This call blocks till all futures in the list have completed.  If the 
   * future completed with an error, the {@link ExecutionException} is swallowed.  
   * Meaning that this does not attempt to verify that all futures completed 
   * successfully.  If you need to know if any failed, please use 
   * <tt>'blockTillAllCompleteOrFirstError'</tt>.
   * 
   * @param futures Structure to iterate over
   * @throws InterruptedException Thrown if thread is interrupted while waiting on future
   */
  public static void blockTillAllComplete(Iterable<Future<?>> futures) throws InterruptedException {
    if (futures == null) {
      return;
    }
    
    Iterator<Future<?>> it = futures.iterator();
    while (it.hasNext()) {
      try {
        it.next().get();
      } catch (ExecutionException e) {
        // swallowed
      }
    }
  }

  /**
   * This call blocks till all futures in the list have completed.  If the 
   * future completed with an error an {@link ExecutionException} is thrown.  
   * If this exception is thrown, all futures may or may not be completed, the 
   * exception is thrown as soon as it is hit.  There also may be additional 
   * futures that errored (but were not hit yet).
   * 
   * @param futures Structure to iterate over
   * @throws InterruptedException Thrown if thread is interrupted while waiting on future
   * @throws ExecutionException Thrown if future throws exception on .get() call
   */
  public static void blockTillAllCompleteOrFirstError(Iterable<Future<?>> futures) throws InterruptedException, 
                                                                                          ExecutionException {
    if (futures == null) {
      return;
    }
    
    Iterator<Future<?>> it = futures.iterator();
    while (it.hasNext()) {
      it.next().get();
    }
  }
}
