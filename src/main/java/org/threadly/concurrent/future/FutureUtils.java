package org.threadly.concurrent.future;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.util.Clock;

/**
 * <p>A collection of small utilities for handling futures.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class FutureUtils {
  /**
   * This call blocks till all futures in the list have completed.  If the future completed with 
   * an error, the {@link ExecutionException} is swallowed.  Meaning that this does not attempt to 
   * verify that all futures completed successfully.  If you need to know if any failed, please 
   * use {@link #blockTillAllCompleteOrFirstError(Iterable)}.  
   * 
   * If you need to specify a timeout to control how long to block, consider using 
   * {@link #blockTillAllComplete(Iterable, long)}.
   * 
   * @param futures Structure of futures to iterate over
   * @throws InterruptedException Thrown if thread is interrupted while waiting on future
   */
  public static void blockTillAllComplete(Iterable<? extends Future<?>> futures) throws InterruptedException {
    countFuturesWithResult(futures, null);
  }
  
  /**
   * This call blocks till all futures in the list have completed.  If the future completed with 
   * an error, the {@link ExecutionException} is swallowed.  Meaning that this does not attempt to 
   * verify that all futures completed successfully.  If you need to know if any failed, please 
   * use {@link #blockTillAllCompleteOrFirstError(Iterable, long)}.
   * 
   * @since 4.0.0
   * 
   * @param futures Structure of futures to iterate over
   * @param timeoutInMillis timeout to wait for futures to complete in milliseconds
   * @throws InterruptedException Thrown if thread is interrupted while waiting on future
   * @throws TimeoutException Thrown if the timeout elapsed while waiting on futures to complete
   */
  public static void blockTillAllComplete(Iterable<? extends Future<?>> futures, long timeoutInMillis) 
      throws InterruptedException, TimeoutException {
    countFuturesWithResult(futures, null, timeoutInMillis);
  }

  /**
   * This call blocks till all futures in the list have completed.  If the future completed with 
   * an error an {@link ExecutionException} is thrown.  If this exception is thrown, all futures 
   * may or may not be completed, the exception is thrown as soon as it is hit.  There also may be 
   * additional futures that errored (but were not hit yet).  
   * 
   * If you need to specify a timeout to control how long to block, consider using 
   * {@link #blockTillAllCompleteOrFirstError(Iterable, long)}.
   * 
   * @param futures Structure of futures to iterate over
   * @throws InterruptedException Thrown if thread is interrupted while waiting on future
   * @throws ExecutionException Thrown if future throws exception on .get() call
   */
  public static void blockTillAllCompleteOrFirstError(Iterable<? extends Future<?>> futures) 
      throws InterruptedException, ExecutionException {
    if (futures == null) {
      return;
    }
    
    Iterator<? extends Future<?>> it = futures.iterator();
    while (it.hasNext()) {
      it.next().get();
    }
  }

  /**
   * This call blocks till all futures in the list have completed.  If the future completed with 
   * an error an {@link ExecutionException} is thrown.  If this exception is thrown, all futures 
   * may or may not be completed, the exception is thrown as soon as it is hit.  There also may be 
   * additional futures that errored (but were not hit yet).
   * 
   * @since 4.0.0
   * 
   * @param futures Structure of futures to iterate over
   * @param timeoutInMillis timeout to wait for futures to complete in milliseconds
   * @throws InterruptedException Thrown if thread is interrupted while waiting on future
   * @throws TimeoutException Thrown if the timeout elapsed while waiting on futures to complete
   * @throws ExecutionException Thrown if future throws exception on .get() call
   */
  public static void blockTillAllCompleteOrFirstError(Iterable<? extends Future<?>> futures, long timeoutInMillis) 
      throws InterruptedException, TimeoutException, ExecutionException {
    if (futures == null) {
      return;
    }
    
    Iterator<? extends Future<?>> it = futures.iterator();
    long startTime = Clock.accurateForwardProgressingMillis();
    long remainingTime;
    while (it.hasNext() && 
           (remainingTime = timeoutInMillis - (Clock.lastKnownForwardProgressingMillis() - startTime)) > 0) {
      it.next().get(remainingTime, TimeUnit.MILLISECONDS);
    }
    if (it.hasNext()) {
      throw new TimeoutException();
    }
  }
  
  /**
   * Counts how many futures provided completed with a result that matches the one provided here.  
   * This can be most useful if your looking to know if an error occurred that was not an 
   * {@link ExecutionException}.  For example assume an API return's {@code Future<Boolean>} and a 
   * {@code false} represents a failure, this can be used to look for those types of error 
   * results.  
   * 
   * Just like {@link #blockTillAllComplete(Iterable)}, this will block until all futures have 
   * completed (so we can verify if their result matches or not).  
   * 
   * If you need to specify a timeout to control how long to block, consider using 
   * {@link #countFuturesWithResult(Iterable, Object, long)}.
   * 
   * @since 4.0.0
   * 
   * @param <T> type of result futures provide to compare against
   * @param futures Structure of futures to iterate over
   * @param comparisonResult Object to compare future results against to look for match
   * @return Number of futures which match the result using a {@link Object#equals(Object)} comparison
   * @throws InterruptedException Thrown if thread is interrupted while waiting on future's result
   */
  public static <T> int countFuturesWithResult(Iterable<? extends Future<?>> futures, T comparisonResult) 
      throws InterruptedException {
    if (futures == null) {
      return 0;
    }
    
    int resultCount = 0;
    Iterator<? extends Future<?>> it = futures.iterator();
    while (it.hasNext()) {
      Future<?> f = it.next();
      if (f.isCancelled()) {
        continue;
      }
      try {
        if (comparisonResult == null) {
          if (f.get() == null) {
            resultCount++;
          }
        } else if (comparisonResult.equals(f.get())) {
          resultCount++;
        }
      } catch (CancellationException e) {
        // swallowed
      } catch (ExecutionException e) {
        // swallowed
      }
    }
    
    return resultCount;
  }
  
  /**
   * Counts how many futures provided completed with a result that matches the one provided here.  
   * This can be most useful if your looking to know if an error occurred that was not an 
   * {@link ExecutionException}.  For example assume an API return's {@code Future<Boolean>} and a 
   * {@code false} represents a failure, this can be used to look for those types of error 
   * results.  
   * 
   * Just like {@link #blockTillAllComplete(Iterable)}, this will block until all futures have 
   * completed (so we can verify if their result matches or not).
   * 
   * @since 4.0.0
   * 
   * @param <T> type of result futures provide to compare against
   * @param futures Structure of futures to iterate over
   * @param comparisonResult Object to compare future results against to look for match
   * @param timeoutInMillis timeout to wait for futures to complete in milliseconds
   * @return Number of futures which match the result using a {@link Object#equals(Object)} comparison
   * @throws InterruptedException Thrown if thread is interrupted while waiting on future's result
   * @throws TimeoutException Thrown if the timeout elapsed while waiting on futures to complete
   */
  public static <T> int countFuturesWithResult(Iterable<? extends Future<?>> futures, 
                                               T comparisonResult, long timeoutInMillis) throws InterruptedException, 
                                                                                                TimeoutException {
    if (futures == null) {
      return 0;
    }
    
    int resultCount = 0;
    Iterator<? extends Future<?>> it = futures.iterator();
    long startTime = Clock.accurateForwardProgressingMillis();
    long remainingTime;
    while (it.hasNext() && 
           (remainingTime = timeoutInMillis - (Clock.lastKnownForwardProgressingMillis() - startTime)) > 0) {
      Future<?> f = it.next();
      try {
        if (comparisonResult == null) {
          if (f.get(remainingTime, TimeUnit.MILLISECONDS) == null) {
            resultCount++;
          }
        } else if (comparisonResult.equals(f.get(remainingTime, TimeUnit.MILLISECONDS))) {
          resultCount++;
        }
      } catch (CancellationException e) {
        // swallowed
      } catch (ExecutionException e) {
        // swallowed
      }
    }
    if (it.hasNext()) {
      throw new TimeoutException();
    }
    
    return resultCount;
  }
  
  /**
   * An alternative to {@link #blockTillAllComplete(Iterable)}, this provides the ability to know 
   * when all futures are complete without blocking.  Unlike 
   * {@link #blockTillAllComplete(Iterable)}, this requires that you provide a collection of 
   * {@link ListenableFuture}'s.  But will return immediately, providing a new 
   * {@link ListenableFuture} that will be called once all the provided futures have finished.  
   * 
   * The future returned will provide a {@code null} result, it is the responsibility of the 
   * caller to get the actual results from the provided futures.  This is designed to just be an 
   * indicator as to when they have finished.  If you need the results from the provided futures, 
   * consider using {@link #makeCompleteListFuture(Iterable)}.
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
   * An alternative to {@link #blockTillAllComplete(Iterable)}, this provides the ability to know 
   * when all futures are complete without blocking.  Unlike 
   * {@link #blockTillAllComplete(Iterable)}, this requires that you provide a collection of 
   * {@link ListenableFuture}'s.  But will return immediately, providing a new 
   * {@link ListenableFuture} that will be called once all the provided futures have finished.  
   * 
   * The future returned will provide the result object once all provided futures have completed.  
   * This call does nothing to provide the actual results from {@link ListenableFuture}'s which 
   * were passed in.  If that is needed consider using {@link #makeCompleteListFuture(Iterable)}.
   * 
   * @since 3.3.0
   * 
   * @param <T> type of result returned from the future
   * @param futures Collection of futures that must finish before returned future is satisfied
   * @param result Result to provide returned future once all futures complete
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static <T> ListenableFuture<T> makeCompleteFuture(Iterable<? extends ListenableFuture<?>> futures, 
                                                           final T result) {
    final EmptyFutureCollection efc = new EmptyFutureCollection(futures);
    final SettableListenableFuture<T> resultFuture = new CancelDelegateSettableListenableFuture<T>(efc);
    efc.addCallback(new FutureCallback<Object>() {
      @Override
      public void handleResult(Object ignored) {
        resultFuture.setResult(result);
      }

      @Override
      public void handleFailure(Throwable t) {
        if (t instanceof CancellationException) {
          // caused by user canceling returned CancelDelegateSettableListenableFuture
        } else {
          resultFuture.setFailure(t);
        }
      }
    });
    return resultFuture;
  }
  
  /**
   * This call is similar to {@link #makeCompleteFuture(Iterable)} in that it will immediately 
   * provide a future that will not be satisfied till all provided futures complete.  
   * 
   * This future provides a list of the completed futures as the result.  The order of this list 
   * is NOT deterministic.
   * 
   * If {@link ListenableFuture#cancel(boolean)} is invoked on the returned future, all provided 
   * futures will attempt to be canceled in the same way.
   * 
   * @since 1.2.0
   * 
   * @param <T> The result object type returned from the futures
   * @param futures Structure of futures to iterate over
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static <T> ListenableFuture<List<ListenableFuture<? extends T>>> 
      makeCompleteListFuture(Iterable<? extends ListenableFuture<? extends T>> futures) {
    return new AllFutureCollection<T>(futures);
  }
  
  /**
   * This call is similar to {@link #makeCompleteFuture(Iterable)} in that it will immediately 
   * provide a future that will not be satisfied till all provided futures complete.  
   * 
   * This future provides a list of the futures that completed without throwing an exception nor 
   * were canceled.  The order of the resulting list is NOT deterministic.
   * 
   * If {@link ListenableFuture#cancel(boolean)} is invoked on the returned future, all provided 
   * futures will attempt to be canceled in the same way.
   * 
   * @since 1.2.0
   * 
   * @param <T> The result object type returned from the futures
   * @param futures Structure of futures to iterate over
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static <T> ListenableFuture<List<ListenableFuture<? extends T>>> 
      makeSuccessListFuture(Iterable<? extends ListenableFuture<? extends T>> futures) {
    return new SuccessFutureCollection<T>(futures);
  }
  
  /**
   * This call is similar to {@link #makeCompleteFuture(Iterable)} in that it will immediately 
   * provide a future that will not be satisfied till all provided futures complete.  
   * 
   * This future provides a list of the futures that failed by either throwing an exception or 
   * were canceled.  The order of the resulting list is NOT deterministic.
   * 
   * If {@link ListenableFuture#cancel(boolean)} is invoked on the returned future, all provided 
   * futures will attempt to be canceled in the same way.
   * 
   * @since 1.2.0
   * 
   * @param <T> The result object type returned from the futures
   * @param futures Structure of futures to iterate over
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static <T> ListenableFuture<List<ListenableFuture<? extends T>>> 
      makeFailureListFuture(Iterable<? extends ListenableFuture<? extends T>> futures) {
    return new FailureFutureCollection<T>(futures);
  }
  
  /**
   * This returns a future which provides the results of all the provided futures.  Thus 
   * preventing the need to iterate over all the futures and manually extract the results.  This 
   * call does NOT block, instead it will return a future which will not complete until all the 
   * provided futures complete.  
   * 
   * The order of the result list is NOT deterministic.
   * 
   * If called with {@code true} for {@code ignoreFailedFutures}, even if some of the provided 
   * futures finished in error, they will be ignored and just the successful results will be 
   * provided.  If called with {@code false} then if any futures complete in error, then the 
   * returned future will throw a {@link ExecutionException} with the error as the cause when 
   * {@link Future#get()} is invoked.  In addition if called with {@code false} and any of the 
   * provided futures are canceled, then the returned future will also be canceled, resulting in a 
   * {@link CancellationException} being thrown when {@link Future#get()} is invoked.  In the case 
   * where there is canceled and failed exceptions in the collection, this will prefer to throw the 
   * failure as an {@link ExecutionException} rather than obscure it with a 
   * {@link CancellationException}.  In other words {@link CancellationException} will be thrown 
   * ONLY if there was canceled tasks, but NO tasks which finished in error.
   * 
   * @since 4.0.0
   * 
   * @param <T> The result object type returned from the futures
   * @param futures Structure of futures to iterate over and extract results from
   * @param ignoreFailedFutures {@code true} to ignore any failed or canceled futures
   * @return A {@link ListenableFuture} which will provide a list of the results from the provided futures
   */
  public static <T> ListenableFuture<List<T>> 
      makeResultListFuture(Iterable<? extends ListenableFuture<? extends T>> futures, 
                           final boolean ignoreFailedFutures) {
    if (futures == null) {
      return immediateResultFuture(Collections.<T>emptyList());
    }
    
    ListenableFuture<List<ListenableFuture<? extends T>>> completeFuture = makeCompleteListFuture(futures);
    final SettableListenableFuture<List<T>> result;
    result = new CancelDelegateSettableListenableFuture<List<T>>(completeFuture);
    
    completeFuture.addCallback(new FutureCallback<List<ListenableFuture<? extends T>>>() {
      @Override
      public void handleResult(List<ListenableFuture<? extends T>> resultFutures) {
        boolean needToCancel = false;
        ArrayList<T> results = new ArrayList<T>(resultFutures.size());
        Iterator<ListenableFuture<? extends T>> it = resultFutures.iterator();
        while (it.hasNext()) {
          ListenableFuture<? extends T> f = it.next();
          if (f.isCancelled()) {
            if (! ignoreFailedFutures) {
              needToCancel = true; // mark to cancel, but search for failure before actually canceling
            }
            continue;
          }
          try {
            results.add(f.get());
          } catch (ExecutionException e) {
            if (! ignoreFailedFutures) {
              result.setFailure(e.getCause());
              return;
            }
          } catch (Exception e) {
            // should not be possible, future is done, cancel checked first, and ExecutionException caught
            result.setFailure(new Exception(e));
            return;
          }
        }
        if (needToCancel) {
          result.cancel(false);
        } else {
          result.setResult(results);
        }
      }

      @Override
      public void handleFailure(Throwable t) {
        if (t instanceof CancellationException) {
          // caused by user canceling returned CancelDelegateSettableListenableFuture
        } else {
          result.setFailure(t);
        }
      }
    });
    
    return result;
  }
  
  /**
   * Invoked {@link Future#cancel(boolean)} for every future in this collection.  Thus if there 
   * are any futures which have not already completed, they will now be marked as canceled.
   * 
   * @param futures Collection of futures to iterate through and cancel
   * @param interruptThread Valued passed in to interrupt thread when calling {@link Future#cancel(boolean)}
   */
  public static void cancelIncompleteFutures(Iterable<? extends Future<?>> futures, 
                                             boolean interruptThread) {
    if (futures == null) {
      return;
    }
    for (Future<?> f : futures) {
      f.cancel(interruptThread);
    }
  }
  
  /**
   * Provide a group of futures and cancel all of them if any of them are canceled or fail.  
   * 
   * If {@code false} is provided for {@code copy} parameter, then {@code futures} will be 
   * iterated over twice, once during this invocation, and again when needing to cancel the 
   * futures.  Because of that it is critical the {@link Iterable} provided returns the exact same 
   * future contents at the time of invoking this call.  If that guarantee can not be provided, 
   * you must specify {@code true} for the {@code copy} parameter.
   * 
   * @since 4.7.2
   * 
   * @param copy {@code true} to copy provided futures to avoid
   * @param futures Futures to be monitored and canceled on error
   * @param interruptThread Valued passed in to interrupt thread when calling {@link Future#cancel(boolean)}
   */
  public static void cancelIncompleteFuturesIfAnyFail(boolean copy, 
                                                      Iterable<? extends ListenableFuture<?>> futures, 
                                                      final boolean interruptThread) {
    if (futures == null) {
      return;
    }
    
    final ArrayList<ListenableFuture<?>> futuresCopy;
    final Iterable<? extends ListenableFuture<?>> callbackFutures;
    if (copy) {
      callbackFutures = futuresCopy = new ArrayList<ListenableFuture<?>>();
    } else {
      futuresCopy = null;
      callbackFutures = futures;
    }
    CancelOnErrorFutureCallback cancelingCallback = 
        new CancelOnErrorFutureCallback(callbackFutures, interruptThread);
    for (ListenableFuture<?> f : futures) {
      if (copy) {
        futuresCopy.add(f);
      }
      f.addCallback(cancelingCallback);
    }
  }
  
  /**
   * Constructs a {@link ListenableFuture} that has already had the provided result given to it.  
   * Thus the resulting future can not error, block, or be canceled.  
   * 
   * If {@code null} is provided here the static instance of 
   * {@link ImmediateResultListenableFuture#NULL_RESULT} will be returned to reduce GC overhead.
   * 
   * @since 1.2.0
   * 
   * @param <T> The result object type returned by the returned future
   * @param result result to be provided in .get() call
   * @return Already satisfied future
   */
  @SuppressWarnings("unchecked")
  public static <T> ListenableFuture<T> immediateResultFuture(T result) {
    if (result == null) {
      return (ListenableFuture<T>)ImmediateResultListenableFuture.NULL_RESULT;
    } else {
      return new ImmediateResultListenableFuture<T>(result);
    }
  }
  
  /**
   * Constructs a {@link ListenableFuture} that has failed with the given failure.  Thus the 
   * resulting future can not block, or be canceled.  Calls to {@link ListenableFuture#get()} will 
   * immediately throw an {@link ExecutionException}.
   * 
   * @since 1.2.0
   * 
   * @param <T> The result object type returned by the returned future
   * @param failure to provide as cause for ExecutionException thrown from .get() call
   * @return Already satisfied future
   */
  public static <T> ListenableFuture<T> immediateFailureFuture(Throwable failure) {
    return new ImmediateFailureListenableFuture<T>(failure);
  }
  
  /**
   * <p>Implementation of {@link SettableListenableFuture} which delegates it's cancel operation 
   * to a parent future.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.1.0
   * @param <T> The result object type returned from the futures
   */
  protected static class CancelDelegateSettableListenableFuture<T> extends SettableListenableFuture<T> {
    private final ListenableFuture<?> cancelDelegateFuture;
    
    protected CancelDelegateSettableListenableFuture(ListenableFuture<?> lf) {
      super(false);
      cancelDelegateFuture = lf;
    }
    
    @Override
    public boolean cancel(boolean interruptThread) {
      if (cancelDelegateFuture.cancel(interruptThread)) {
        return super.cancel(interruptThread);
      } else {
        return false;
      }
    }
  }
  
  /**
   * <p>A future implementation that will return a List of futures as the result.  The future will 
   * not be satisfied till all provided futures have completed.</p>
   * 
   * @author jent - Mike Jensn
   * @since 1.2.0
   * @param <T> The result object type returned from the futures
   */
  protected abstract static class FutureCollection<T> 
      extends SettableListenableFuture<List<ListenableFuture<? extends T>>> {
    protected final AtomicInteger remainingResult;
    private final AtomicReference<ConcurrentArrayList<ListenableFuture<? extends T>>> buildingResult;
    private ArrayList<ListenableFuture<? extends T>> futures;
    
    protected FutureCollection(Iterable<? extends ListenableFuture<? extends T>> source) {
      super(false);
      remainingResult = new AtomicInteger(0); // may go negative if results finish before all are added
      buildingResult = new AtomicReference<ConcurrentArrayList<ListenableFuture<? extends T>>>(null);
      futures = new ArrayList<ListenableFuture<? extends T>>();
      
      if (source != null) {
        Iterator<? extends ListenableFuture<? extends T>> it = source.iterator();
        while (it.hasNext()) {
          ListenableFuture<? extends T> f = it.next();
          futures.add(f);
          attachFutureDoneTask(f);
        }
      }
      
      futures.trimToSize();
      
      // we need to verify that all futures have not already completed
      if (remainingResult.addAndGet(futures.size()) == 0) {
        setResult(getFinalResultList());
      }
      
      addListener(new Runnable() {
        @Override
        public void run() {
          futures = null;
        }
      });
    }
    
    /**
     * Attach a {@link FutureDoneTask} to the provided future.  This is necessary for tracking as 
     * futures complete, failing to attach a task could result in this future never completing.  
     * 
     * This is provided as a separate function so it can be overriden to provide different 
     * {@link FutureDoneTask} implementation.
     * 
     * @param f Future to attach to
     */
    protected void attachFutureDoneTask(ListenableFuture<? extends T> f) {
      f.addListener(new FutureDoneTask(f));
    }
    
    @Override
    public boolean cancel(boolean interrupt) {
      // we need a copy in case canceling clears out the futures
      ArrayList<ListenableFuture<? extends T>> futures = this.futures;
      if (super.cancel(interrupt)) {
        cancelIncompleteFutures(futures, interrupt);
        return true;
      } else {
        return false;
      }
    }
    
    /**
     * Adds item to the result list.  This list may be lazily constructed and thus why you must add 
     * through this function rather than directly on to the list.
     */
    protected void addResult(ListenableFuture<? extends T> f) {
      List<ListenableFuture<? extends T>> list = buildingResult.get();
      
      if (list == null) {
        int rearPadding = remainingResult.get();
        if (rearPadding < 0) {
          rearPadding *= -1;
        }
        
        ConcurrentArrayList<ListenableFuture<? extends T>> newList;
        newList = new ConcurrentArrayList<ListenableFuture<? extends T>>(0, rearPadding);
        
        if (buildingResult.compareAndSet(null, newList)) {
          list = newList;
          list.add(f);  // must add before updating the rear padding
          if (rearPadding > 2) {
            // set back to reasonable number after construction in hopes that we wont have to expand much
            newList.setRearPadding(2);
          }
          return; // return so we don't add again
        } else {
          list = buildingResult.get();
        }
      }
      
      list.add(f);
    }
    
    /**
     * Gives the implementing class the option to save or check the completed future.
     * 
     * @param f {@link ListenableFuture} that has completed
     */
    protected abstract void handleFutureDone(ListenableFuture<? extends T> f);

    /**
     * Will only be called once, and all allocated resources can be freed after this point.
     * 
     * @return List to satisfy ListenableFuture result with
     */
    protected List<ListenableFuture<? extends T>> getFinalResultList() {
      ConcurrentArrayList<ListenableFuture<? extends T>> resultsList = buildingResult.get();
      if (resultsList == null) {
        return Collections.emptyList();
      } else {
        buildingResult.lazySet(null);
        resultsList.trimToSize();
        return Collections.unmodifiableList(resultsList);
      }
    }
    
    /**
     * <p>Task which is ran after a future completes.  This is used internally to track how many 
     * outstanding tasks are remaining, as well as used to collect the results if desired.</p>
     * 
     * @author jent - Mike Jensen
     * @since 4.7.0
     */
    protected class FutureDoneTask implements Runnable {
      private final ListenableFuture<? extends T> f;
      
      protected FutureDoneTask(ListenableFuture<? extends T> f) {
        this.f = f;
      }
      
      @Override
      public void run() {
        try {  // exceptions should not be possible, but done for robustness
          handleFutureDone(f);
        } finally {
          // all futures are now done
          if (remainingResult.decrementAndGet() == 0) {
            setResult(getFinalResultList());
          }
        }
      }
    }
  }
  
  /**
   * <p>A future implementation that will be satisfied till all provided futures have 
   * completed.</p>
   * 
   * @author jent - Mike Jensn
   * @since 1.2.0
   */
  protected static class EmptyFutureCollection extends FutureCollection<Object> {
    private FutureDoneTask doneTaskSingleton = null;
    
    protected EmptyFutureCollection(Iterable<? extends ListenableFuture<?>> source) {
      super(source);
    }

    @Override
    protected void handleFutureDone(ListenableFuture<?> f) {
      // ignored
    }
    
    @Override
    protected List<ListenableFuture<?>> getFinalResultList() {
      return null;
    }
    
    @Override
    protected void attachFutureDoneTask(ListenableFuture<?> f) {
      // we don't care about the result of the future
      // so to save a little memory we reuse the same task with no future provided
      if (doneTaskSingleton == null) {
        doneTaskSingleton = new FutureDoneTask(null);
      }
      
      f.addListener(doneTaskSingleton);
    }
  }
  
  /**
   * <p>A future implementation that will return a List of futures as the result.  The future will 
   * not be satisfied till all provided futures have completed.</p>
   * 
   * <p>This implementation will return a result of all the futures that completed.</p>
   * 
   * @author jent - Mike Jensn
   * @since 1.2.0
   * @param <T> The result object type returned from the futures
   */
  protected static class AllFutureCollection<T> extends FutureCollection<T> {
    protected AllFutureCollection(Iterable<? extends ListenableFuture<? extends T>> source) {
      super(source);
    }

    @Override
    protected void handleFutureDone(ListenableFuture<? extends T> f) {
      addResult(f);
    }
  }
  
  /**
   * <p>A future implementation that will return a List of futures as the result.  The future will 
   * not be satisfied till all provided futures have completed.</p>
   * 
   * <p>This implementation will return a result of all the futures that completed successfully.  
   * If the future was canceled or threw an exception it will not be included.</p>
   * 
   * @author jent - Mike Jensn
   * @since 1.2.0
   * @param <T> The result object type returned from the futures
   */
  protected static class SuccessFutureCollection<T> extends AllFutureCollection<T> {
    protected SuccessFutureCollection(Iterable<? extends ListenableFuture<? extends T>> source) {
      super(source);
    }

    @Override
    protected void handleFutureDone(ListenableFuture<? extends T> f) {
      if (f.isCancelled()) {
        // detect canceled conditions before an exception would have otherwise thrown
        // canceled futures are ignored
        return;
      }
      try {
        f.get();
        
        // if no exception thrown, add future
        super.handleFutureDone(f);
      } catch (InterruptedException e) {
        /* should not be possible since this should only 
         * be called once the future is already done
         */
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        // ignored
      } catch (CancellationException e) {
        // should not be possible due check at start on what should be an already done future
        throw e;
      }
    }
  }
  
  /**
   * <p>A future implementation that will return a List of futures as the result.  The future will 
   * not be satisfied till all provided futures have completed.</p>
   * 
   * <p>This implementation will return a result of all the futures that either threw an exception 
   * during computation, or was canceled.</p>
   * 
   * @author jent - Mike Jensn
   * @since 1.2.0
   * @param <T> The result object type returned from the futures
   */
  protected static class FailureFutureCollection<T> extends AllFutureCollection<T> {
    protected FailureFutureCollection(Iterable<? extends ListenableFuture<? extends T>> source) {
      super(source);
    }

    @Override
    protected void handleFutureDone(ListenableFuture<? extends T> f) {
      if (f.isCancelled()) {
        // detect canceled conditions before an exception would have otherwise thrown 
        super.handleFutureDone(f);
        return;
      }
      try {
        f.get();
      } catch (InterruptedException e) {
        /* should not be possible since this should only 
         * be called once the future is already done
         */
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        // failed so add it
        super.handleFutureDone(f);
      } catch (CancellationException e) {
        // should not be possible due check at start on what should be an already done future
        throw e;
      }
    }
  }
  
  /**
   * <p>Future callback that on error condition will cancel all the provided futures.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.7.2
   */
  protected static class CancelOnErrorFutureCallback extends AbstractFutureCallbackFailureHandler {
    private final Iterable<? extends ListenableFuture<?>> futures;
    private final boolean interruptThread;
    private final AtomicBoolean canceled;
    
    public CancelOnErrorFutureCallback(Iterable<? extends ListenableFuture<?>> futures, 
                                       boolean interruptThread) {
      this.futures = futures;
      this.interruptThread = interruptThread;
      this.canceled = new AtomicBoolean(false);
    }

    @Override
    public void handleFailure(Throwable t) {
      if (! canceled.get() && canceled.compareAndSet(false, true)) {
        cancelIncompleteFutures(futures, interruptThread);
      }
    }
  }
}
