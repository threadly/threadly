package org.threadly.concurrent.future;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>A collection of small utilities for handling futures.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
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
  public static void blockTillAllComplete(Iterable<? extends Future<?>> futures) throws InterruptedException {
    if (futures == null) {
      return;
    }
    
    Iterator<? extends Future<?>> it = futures.iterator();
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
  public static void blockTillAllCompleteOrFirstError(Iterable<? extends Future<?>> futures) 
      throws InterruptedException, 
             ExecutionException {
    if (futures == null) {
      return;
    }
    
    Iterator<? extends Future<?>> it = futures.iterator();
    while (it.hasNext()) {
      it.next().get();
    }
  }
  
  /**
   * An alternative to blockTillAllComplete, this provides the ability to know when 
   * all futures are complete without blocking.  Unlike blockTillAllComplete, this 
   * requires that your provide a collection of {@link ListenableFuture}'s.  But will 
   * return immediately, providing a new ListenableFuture that will be called once 
   * all the provided futures have finished.  
   * 
   * The future returned will return a null result, it is the responsibility of the 
   * caller to get the actual results from the provided futures.  This is designed to 
   * just be an indicator as to when they have finished.  
   * 
   * It is also important that the provided collection is not modified while this is 
   * being called.  If it is, the returned future's behavior is undefined (it may 
   * never complete, or it may complete early).
   * 
   * @param futures Collection of futures that must finish before returned future is satisfied
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static ListenableFuture<?> makeAllCompleteFuture(Collection<? extends ListenableFuture<?>> futures) {
    final ListenableFutureResult<?> result = new ListenableFutureResult<Object>();
    
    if (futures == null || futures.isEmpty()) {
      result.setResult(null);
    } else {
      final AtomicInteger remainingFutures = new AtomicInteger(futures.size());
      
      Iterator<? extends ListenableFuture<?>> it = futures.iterator();
      while (it.hasNext()) {
        it.next().addListener(new Runnable() {
          @Override
          public void run() {
            if (remainingFutures.decrementAndGet() == 0) {
              result.setResult(null);
            }
          }
        });
      }
    }
    
    return result;
  }
}
