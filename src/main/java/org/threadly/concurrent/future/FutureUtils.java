package org.threadly.concurrent.future;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.threadly.concurrent.collections.ConcurrentArrayList;

/**
 * <p>A collection of small utilities for handling futures.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class FutureUtils {
  /**
   * Adds a callback to a given future to be called once the future completes.
   * Please see addListener in {@link ListenableFuture} to understand more about 
   * how these callbacks are called.
   * 
   * The callback of this call will be called either on this thread (if the future 
   * has already completed), or on the resulting thread.  If the callback has 
   * high complexity, consider passing an executor in for it to be called on.
   * 
   * @since 1.2.0
   * 
   * @param <T> type of result returned from the future
   * @param future future to attach callback to
   * @param callback callback to call once future completes
   */
  public static <T> void addCallback(final ListenableFuture<T> future, 
                                     FutureCallback<? super T> callback) {
    addCallback(future, callback, null);
  }
  
  /**
   * Adds a callback to a given future to be called once the future completes.
   * Please see addListener in {@link ListenableFuture} to understand more about 
   * how these callbacks are called.
   * 
   * @since 1.2.0
   * 
   * @param <T> type of result returned from the future
   * @param future future to attach callback to
   * @param callback callback to call once future completes
   * @param executor executor to call callback on
   */
  public static <T> void addCallback(final ListenableFuture<T> future, 
                                     final FutureCallback<? super T> callback, 
                                     Executor executor) {
    if (future == null) {
      throw new IllegalArgumentException("Must provide future for callback to listen on");
    } else if (callback == null) {
      throw new IllegalArgumentException("Must provide callback to call into");
    }
    future.addListener(new Runnable() {
      @Override
      public void run() {
        try {
          T result = future.get();
          callback.handleResult(result);
        } catch (InterruptedException e) {
          // should not be possible as future already has result
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          callback.handleFailure(e.getCause());
        } catch (CancellationException e) {
          callback.handleFailure(e);
        }
      }
    }, executor);
  }
  
  /**
   * This call blocks till all futures in the list have completed.  If the 
   * future completed with an error, the {@link ExecutionException} is swallowed.  
   * Meaning that this does not attempt to verify that all futures completed 
   * successfully.  If you need to know if any failed, please use 
   * <tt>'blockTillAllCompleteOrFirstError'</tt>.
   * 
   * @param futures Structure of futures to iterate over
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
   * @param futures Structure of futures to iterate over
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
   * @since 1.2.0
   * 
   * @param futures Collection of futures that must finish before returned future is satisfied
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static ListenableFuture<?> makeCompleteFuture(Iterable<? extends ListenableFuture<?>> futures) {
    return new EmptyFutureCollection(futures);
  }
  
  /**
   * This call is similar to makeCompleteFuture in that it will immediately provide a 
   * future that can not be canceled, and will not be satifised till all provided 
   * futures complete.  
   * 
   * This future provides a list of the completed futures as the result.  The order 
   * of this list is NOT deterministic.
   * 
   * @since 1.2.0
   * 
   * @param <T> type of result returned from the futures
   * @param futures Structure of futures to iterate over
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static <T> ListenableFuture<List<ListenableFuture<? extends T>>> 
      makeCompleteListFuture(Iterable<? extends ListenableFuture<? extends T>> futures) {
    return new AllFutureCollection<T>(futures);
  }
  
  /**
   * This call is similar to makeCompleteFuture in that it will immediately provide a 
   * future that can not be canceled, and will not be satifised till all provided 
   * futures complete.  
   * 
   * This future provides a list of the futures that completed without throwing 
   * an exception nor were canceled.  The order of the resulting list is NOT 
   * deterministic.
   * 
   * @since 1.2.0
   * 
   * @param <T> type of result returned from the futures
   * @param futures Structure of futures to iterate over
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static <T> ListenableFuture<List<ListenableFuture<? extends T>>> 
      makeSuccessListFuture(Iterable<? extends ListenableFuture<? extends T>> futures) {
    return new SuccessFutureCollection<T>(futures);
  }
  
  /**
   * This call is similar to makeCompleteFuture in that it will immediately provide a 
   * future that can not be canceled, and will not be satifised till all provided 
   * futures complete.  
   * 
   * This future provides a list of the futures that failed by either throwing an 
   * exception or were canceled.  The order of the resulting list is NOT 
   * deterministic.
   * 
   * @since 1.2.0
   * 
   * @param <T> type of result returned from the futures
   * @param futures Structure of futures to iterate over
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static <T> ListenableFuture<List<ListenableFuture<? extends T>>> 
      makeFailureListFuture(Iterable<? extends ListenableFuture<? extends T>> futures) {
    return new FailureFutureCollection<T>(futures);
  }
  
  /**
   * Constructs a {@link ListenableFuture} that has already had the 
   * provided result given to it.  Thus the resulting future can not 
   * error, block, or be canceled.
   * 
   * @since 1.2.0
   * 
   * @param <T> type of result returned from the future
   * @param result result to be provided in .get() call
   * @return Already satisfied future
   */
  public static <T> ListenableFuture<T> immediateResultFuture(T result) {
    return new ImmediateResultListenableFuture<T>(result);
  }
  
  /**
   * Constructs a {@link ListenableFuture} that has failed with the 
   * given failure.  Thus the resulting future can not block, or be 
   * canceled.  Calls to .get() will immediately throw an 
   * ExecutionException.
   * 
   * @since 1.2.0
   * 
   * @param <T> type of result returned from the future
   * @param failure to provide as cause for ExecutionException thrown from .get() call
   * @return Already satisfied future
   */
  public static <T> ListenableFuture<T> immediateFailureFuture(Throwable failure) {
    return new ImmediateFailureListenableFuture<T>(failure);
  }
  
  /**
   * <p>A future implementation that will return a List of futures as the result.  The 
   * future will not be satisfied till all provided futures have completed.</p>
   * 
   * @author jent - Mike Jensn
   * @since 1.2.0
   * @param <T> type of result returned from the futures
   */
  protected abstract static class FutureCollection<T> 
      extends SettableListenableFuture<List<ListenableFuture<? extends T>>> {
    protected final AtomicInteger remainingResult;
    private final AtomicReference<List<ListenableFuture<? extends T>>> buildingResult;
    
    protected FutureCollection(Iterable<? extends ListenableFuture<? extends T>> source) {
      remainingResult = new AtomicInteger(0); // may go negative if results finish before all are added
      buildingResult = new AtomicReference<List<ListenableFuture<? extends T>>>(null);
      
      int expectedResultCount = 0;
      if (source != null) {
        Iterator<? extends ListenableFuture<? extends T>> it = source.iterator();
        while (it.hasNext()) {
          expectedResultCount++;
          
          final ListenableFuture<? extends T> f = it.next();
          f.addListener(new Runnable() {
            @Override
            public void run() {
              handleFutureDone(f);
              
              // all futures are now done
              if (remainingResult.decrementAndGet() == 0) {
                setResult(getFinalResultList());
              }
            }
          });
        }
      }
      
      // we need to verify that all futures have not already completed
      if (remainingResult.addAndGet(expectedResultCount) == 0) {
        setResult(getFinalResultList());
      }
    }
    
    /**
     * Provides the lazily constructed buildingResult in case any futures 
     * need to be saved.  This is complex because it may be called very 
     * early, and we try to keep this as efficient as possible.
     * 
     * @return A stored list of futures that can be modified
     */
    protected List<ListenableFuture<? extends T>> getBuildingResult() {
      List<ListenableFuture<? extends T>> result = buildingResult.get();
      
      if (result == null) {
        int rearPadding = remainingResult.get();
        if (rearPadding < 0) {
          rearPadding *= -1;
        }
        
        ConcurrentArrayList<ListenableFuture<? extends T>> resultList;
        resultList = new ConcurrentArrayList<ListenableFuture<? extends T>>(0, rearPadding);
        
        if (buildingResult.compareAndSet(null, resultList)) {
          result = resultList;
          if (rearPadding != 0) {
            // set back to zero after construction in hopes that we wont have to expand much
            resultList.setRearPadding(0);
          }
        } else {
          result = buildingResult.get();
        }
      }
      
      return result;
    }
    
    /**
     * Gives the implementing class the option to save or check the completed 
     * future.
     * 
     * @param f {@link ListenableFuture} that has completed
     */
    protected abstract void handleFutureDone(ListenableFuture<? extends T> f);

    /**
     * Will only be called once, and all allocated resources can be freed after this
     * point.
     * 
     * @return List to satisfy ListenableFuture result with
     */
    private List<ListenableFuture<? extends T>> getFinalResultList() {
      List<ListenableFuture<? extends T>> result;
      if (buildingResult.get() == null) {
        result = Collections.emptyList();
      } else {
        result = Collections.unmodifiableList(buildingResult.get());
        buildingResult.set(null);
      }
      
      return result;
    }
  }
  
  /**
   * <p>A future implementation that will be satisfied till all provided futures 
   * have completed.</p>
   * 
   * @author jent - Mike Jensn
   * @since 1.2.0
   */
  protected static class EmptyFutureCollection extends FutureCollection<Object> {
    protected EmptyFutureCollection(Iterable<? extends ListenableFuture<?>> source) {
      super(source);
    }

    @Override
    protected void handleFutureDone(ListenableFuture<?> f) {
      // ignored
    }
  }
  
  /**
   * <p>A future implementation that will return a List of futures as the result.  The 
   * future will not be satisfied till all provided futures have completed.</p>
   * 
   * <p>This implementation will return a result of all the futures that completed.</p>
   * 
   * @author jent - Mike Jensn
   * @since 1.2.0
   * @param <T> type of result returned from the futures
   */
  protected static class AllFutureCollection<T> extends FutureCollection<T> {
    protected AllFutureCollection(Iterable<? extends ListenableFuture<? extends T>> source) {
      super(source);
      
      getBuildingResult();
    }

    @Override
    protected void handleFutureDone(ListenableFuture<? extends T> f) {
      getBuildingResult().add(f);
    }
  }
  
  /**
   * <p>A future implementation that will return a List of futures as the result.  The 
   * future will not be satisfied till all provided futures have completed.</p>
   * 
   * <p>This implementation will return a result of all the futures that completed 
   * successfully.  If the future was canceled or threw an exception it will not be 
   * included.</p>
   * 
   * @author jent - Mike Jensn
   * @since 1.2.0
   * @param <T> type of result returned from the futures
   */
  protected static class SuccessFutureCollection<T> extends AllFutureCollection<T> {
    protected SuccessFutureCollection(Iterable<? extends ListenableFuture<? extends T>> source) {
      super(source);
    }

    @Override
    protected void handleFutureDone(ListenableFuture<? extends T> f) {
      try {
        f.get();
        
        // if no exception thrown, add future
        super.handleFutureDone(f);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        // ignored
      } catch (CancellationException e) {
        // ignored
      }
    }
  }
  
  /**
   * <p>A future implementation that will return a List of futures as the result.  The 
   * future will not be satisfied till all provided futures have completed.</p>
   * 
   * <p>This implementation will return a result of all the futures that either threw 
   * an exception during computation, or was canceled.</p>
   * 
   * @author jent - Mike Jensn
   * @since 1.2.0
   * @param <T> type of result returned from the futures
   */
  protected static class FailureFutureCollection<T> extends AllFutureCollection<T> {
    protected FailureFutureCollection(Iterable<? extends ListenableFuture<? extends T>> source) {
      super(source);
    }

    @Override
    protected void handleFutureDone(ListenableFuture<? extends T> f) {
      try {
        f.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        // failed so add it
        super.handleFutureDone(f);
      } catch (CancellationException e) {
        // canceled so add it
        super.handleFutureDone(f);
      }
    }
  }
}
