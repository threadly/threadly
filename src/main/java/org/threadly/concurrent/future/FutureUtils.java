package org.threadly.concurrent.future;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.future.ListenableFuture.ListenerOptimizationStrategy;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.SuppressedStackRuntimeException;

/**
 * A collection of small utilities for handling futures.  This class has lots of tools for dealing 
 * with collections of futures, either blocking, extracting results, and more.
 * <p>
 * Generating already done futures:
 * <ul>
 * <li>{@link #immediateResultFuture(Object)}
 * <li>{@link #immediateFailureFuture(Throwable)}
 * </ul>
 * <p>
 * Tools for blocking:
 * <ul>
 * <li>{@link #blockTillAllComplete(Iterable)}
 * <li>{@link #blockTillAllComplete(Iterable, long)}
 * <li>{@link #blockTillAllCompleteOrFirstError(Iterable)}
 * <li>{@link #blockTillAllCompleteOrFirstError(Iterable, long)}
 * </ul>
 * <p>
 * Tools for manipulating collections of futures:
 * <ul>
 * <li>{@link #cancelIncompleteFutures(Iterable, boolean)}
 * <li>{@link #cancelIncompleteFuturesIfAnyFail(boolean, Iterable, boolean)}
 * <li>{@link #countFuturesWithResult(Iterable, Object)}
 * <li>{@link #countFuturesWithResult(Iterable, Object, long)}
 * <li>{@link #makeCompleteFuture(Iterable)}
 * <li>{@link #makeCompleteFuture(Iterable, Object)}
 * <li>{@link #makeFailurePropagatingCompleteFuture(Iterable)}
 * <li>{@link #makeFailurePropagatingCompleteFuture(Iterable, Object)}
 * <li>{@link #makeCompleteListFuture(Iterable)}
 * <li>{@link #makeFailureListFuture(Iterable)}
 * <li>{@link #makeResultListFuture(Iterable, boolean)}
 * <li>{@link #makeSuccessListFuture(Iterable)}
 * </ul>
 * <p>
 * Retry operation and return final result in future:
 * <ul>
 * <li>{@link #scheduleWhileTaskResultNull(SubmitterScheduler, long, boolean, Callable)}
 * <li>{@link #scheduleWhileTaskResultNull(SubmitterScheduler, long, boolean, Callable, long)}
 * <li>{@link #scheduleWhile(SubmitterScheduler, long, boolean, Callable, Predicate)}
 * <li>{@link #scheduleWhile(SubmitterScheduler, long, boolean, Callable, Predicate, long, boolean)}
 * <li>{@link #scheduleWhile(SubmitterScheduler, long, ListenableFuture, Callable, Predicate)}
 * <li>{@link #scheduleWhile(SubmitterScheduler, long, ListenableFuture, Callable, Predicate, long, boolean)}
 * <li>{@link #scheduleWhile(SubmitterScheduler, long, boolean, Runnable, Supplier)}
 * <li>{@link #scheduleWhile(SubmitterScheduler, long, boolean, Runnable, Supplier, long)}
 * </ul>
 * 
 * @since 1.0.0
 */
public class FutureUtils {
  /**
   * This call blocks till all futures in the list have completed.  If the future completed with 
   * an error, the {@link ExecutionException} is swallowed.  Meaning that this does not attempt to 
   * verify that all futures completed successfully.  If you need to know if any failed, please 
   * use {@link #blockTillAllCompleteOrFirstError(Iterable)}.  
   * <p>
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
   * <p>
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
   * <p>
   * Just like {@link #blockTillAllComplete(Iterable)}, this will block until all futures have 
   * completed (so we can verify if their result matches or not).  
   * <p>
   * If you need to specify a timeout to control how long to block, consider using 
   * {@link #countFuturesWithResult(Iterable, Object, long)}.
   * 
   * @since 4.0.0
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
   * <p>
   * Just like {@link #blockTillAllComplete(Iterable)}, this will block until all futures have 
   * completed (so we can verify if their result matches or not).
   * 
   * @since 4.0.0
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
   * <p>
   * The future returned will provide a {@code null} result, it is the responsibility of the 
   * caller to get the actual results from the provided futures.  This is designed to just be an 
   * indicator as to when they have finished.  If you need the results from the provided futures, 
   * consider using {@link #makeCompleteListFuture(Iterable)}.  You should also consider using 
   * {@link #makeFailurePropagatingCompleteFuture(Iterable)}, it has the same semantics as this one 
   * except it will put the returned future into an error state if any of the provided futures error.
   * 
   * @since 5.3
   * @param futures Collection of futures that must finish before returned future is satisfied
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static ListenableFuture<?> makeCompleteFuture(List<? extends ListenableFuture<?>> futures) {
    if (futures == null || futures.isEmpty()) {
      return ImmediateResultListenableFuture.NULL_RESULT;
    } else if (futures.size() == 1) {
      return futures.get(0);
    } else {
      return makeCompleteFuture((Iterable<? extends ListenableFuture<?>>)futures);
    }
  }
  
  /**
   * An alternative to {@link #blockTillAllComplete(Iterable)}, this provides the ability to know 
   * when all futures are complete without blocking.  Unlike 
   * {@link #blockTillAllComplete(Iterable)}, this requires that you provide a collection of 
   * {@link ListenableFuture}'s.  But will return immediately, providing a new 
   * {@link ListenableFuture} that will be called once all the provided futures have finished.  
   * <p>
   * The future returned will provide a {@code null} result, it is the responsibility of the 
   * caller to get the actual results from the provided futures.  This is designed to just be an 
   * indicator as to when they have finished.  If you need the results from the provided futures, 
   * consider using {@link #makeCompleteListFuture(Iterable)}.  You should also consider using 
   * {@link #makeFailurePropagatingCompleteFuture(Iterable)}, it has the same semantics as this one 
   * except it will put the returned future into an error state if any of the provided futures error.
   * 
   * @since 1.2.0
   * @param futures Collection of futures that must finish before returned future is satisfied
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static ListenableFuture<?> makeCompleteFuture(Iterable<? extends ListenableFuture<?>> futures) {
    ListenableFuture<?> result = new EmptyFutureCollection(futures);
    if (result.isDone()) {
      // might as well return a cheaper option
      return ImmediateResultListenableFuture.NULL_RESULT;
    } else {
      return result;
    }
  }
  
  /**
   * An alternative to {@link #blockTillAllComplete(Iterable)}, this provides the ability to know 
   * when all futures are complete without blocking.  Unlike 
   * {@link #blockTillAllComplete(Iterable)}, this requires that you provide a collection of 
   * {@link ListenableFuture}'s.  But will return immediately, providing a new 
   * {@link ListenableFuture} that will be called once all the provided futures have finished.  
   * <p>
   * The future returned will provide the result object once all provided futures have completed.  
   * If any failures occured, they will not be represented in the returned future.  If that is 
   * desired you should consider using 
   * {@link #makeFailurePropagatingCompleteFuture(Iterable, Object)}, it has the same semantics as 
   * this one except it will put the returned future into an error state if any of the provided 
   * futures error.
   * 
   * @since 3.3.0
   * @param <T> type of result returned from the future
   * @param futures Collection of futures that must finish before returned future is satisfied
   * @param result Result to provide returned future once all futures complete
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static <T> ListenableFuture<T> makeCompleteFuture(Iterable<? extends ListenableFuture<?>> futures, 
                                                           final T result) {
    final EmptyFutureCollection efc = new EmptyFutureCollection(futures);
    final SettableListenableFuture<T> resultFuture = new CancelDelegateSettableListenableFuture<>(efc);
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
   * Similar to {@link #makeCompleteFuture(Iterable)} in that the returned future wont complete 
   * until all the provided futures complete.  However this implementation will check if any 
   * futures failed or were canceled once all have completed.  If any did not complete normally 
   * then the returned futures state will match the state of one of the futures that did not 
   * normally (randomly chosen).
   * <p>
   * Since the returned future wont complete until all futures complete, you may want to consider 
   * using {@link #cancelIncompleteFuturesIfAnyFail(boolean, Iterable, boolean)} in addition to 
   * this so that the future will resolve as soon as any failures occur. 
   * 
   * @since 5.0
   * @param futures Collection of futures that must finish before returned future is satisfied
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static ListenableFuture<?> 
      makeFailurePropagatingCompleteFuture(Iterable<? extends ListenableFuture<?>> futures) {
    return makeFailurePropagatingCompleteFuture(futures, null);
  }

  /**
   * Similar to {@link #makeCompleteFuture(Iterable, Object)} in that the returned future wont 
   * complete until all the provided futures complete.  However this implementation will check if 
   * any futures failed or were canceled once all have completed.  If any did not complete normally 
   * then the returned futures state will match the state of one of the futures that did not 
   * normally (randomly chosen).
   * <p>
   * Since the returned future wont complete until all futures complete, you may want to consider 
   * using {@link #cancelIncompleteFuturesIfAnyFail(boolean, Iterable, boolean)} in addition to 
   * this so that the future will resolve as soon as any failures occur.
   *
   * @since 5.0
   * @param <T> type of result returned from the future
   * @param futures Collection of futures that must finish before returned future is satisfied
   * @param result Result to provide returned future once all futures complete
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static <T> ListenableFuture<T> 
      makeFailurePropagatingCompleteFuture(Iterable<? extends ListenableFuture<?>> futures, 
                                           final T result) {
    FailureFutureCollection<Object> ffc = new FailureFutureCollection<>(futures);
    final CancelDelegateSettableListenableFuture<T> resultFuture = 
        new CancelDelegateSettableListenableFuture<>(ffc);
    ffc.addCallback(new FutureCallback<List<ListenableFuture<?>>>() {
      @Override
      public void handleResult(List<ListenableFuture<?>> failedFutures) {
        if (failedFutures.isEmpty()) {
          resultFuture.setResult(result);
        } else {
          // propagate error
          ListenableFuture<?> f = failedFutures.get(0);
          if (f.isCancelled()) {
            resultFuture.cancelRegardlessOfDelegateFutureState(false);
          } else {
            try {
              f.get();
            } catch (ExecutionException e) {
              resultFuture.setFailure(e.getCause());
            } catch (InterruptedException e) {
              // should not be possible
              throw new RuntimeException(e);
            }
          }
        }
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
   * <p>
   * This future provides a list of the completed futures as the result.  The order of this list 
   * is NOT deterministic.
   * <p>
   * If {@link ListenableFuture#cancel(boolean)} is invoked on the returned future, all provided 
   * futures will attempt to be canceled in the same way.
   * 
   * @since 1.2.0
   * @param <T> The result object type returned from the futures
   * @param futures Structure of futures to iterate over
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static <T> ListenableFuture<List<ListenableFuture<? extends T>>> 
      makeCompleteListFuture(Iterable<? extends ListenableFuture<? extends T>> futures) {
    return new AllFutureCollection<>(futures);
  }
  
  /**
   * This call is similar to {@link #makeCompleteFuture(Iterable)} in that it will immediately 
   * provide a future that will not be satisfied till all provided futures complete.  
   * <p>
   * This future provides a list of the futures that completed without throwing an exception nor 
   * were canceled.  The order of the resulting list is NOT deterministic.
   * <p>
   * If {@link ListenableFuture#cancel(boolean)} is invoked on the returned future, all provided 
   * futures will attempt to be canceled in the same way.
   * 
   * @since 1.2.0
   * @param <T> The result object type returned from the futures
   * @param futures Structure of futures to iterate over
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static <T> ListenableFuture<List<ListenableFuture<? extends T>>> 
      makeSuccessListFuture(Iterable<? extends ListenableFuture<? extends T>> futures) {
    return new SuccessFutureCollection<>(futures);
  }
  
  /**
   * This call is similar to {@link #makeCompleteFuture(Iterable)} in that it will immediately 
   * provide a future that will not be satisfied till all provided futures complete.  
   * <p>
   * This future provides a list of the futures that failed by either throwing an exception or 
   * were canceled.  The order of the resulting list is NOT deterministic.
   * <p>
   * If {@link ListenableFuture#cancel(boolean)} is invoked on the returned future, all provided 
   * futures will attempt to be canceled in the same way.
   * 
   * @since 1.2.0
   * @param <T> The result object type returned from the futures
   * @param futures Structure of futures to iterate over
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static <T> ListenableFuture<List<ListenableFuture<? extends T>>> 
      makeFailureListFuture(Iterable<? extends ListenableFuture<? extends T>> futures) {
    return new FailureFutureCollection<>(futures);
  }
  
  /**
   * This returns a future which provides the results of all the provided futures.  Thus 
   * preventing the need to iterate over all the futures and manually extract the results.  This 
   * call does NOT block, instead it will return a future which will not complete until all the 
   * provided futures complete.  
   * <p>
   * The order of the result list is NOT deterministic.
   * <p>
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
    result = new CancelDelegateSettableListenableFuture<>(completeFuture);
    
    completeFuture.addCallback(new FutureCallback<List<ListenableFuture<? extends T>>>() {
      @Override
      public void handleResult(List<ListenableFuture<? extends T>> resultFutures) {
        boolean needToCancel = false;
        ArrayList<T> results = new ArrayList<>(resultFutures.size());
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
          if (! result.cancel(false)) {
            throw new IllegalStateException();
          }
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
   * <p>
   * If {@code false} is provided for {@code copy} parameter, then {@code futures} will be 
   * iterated over twice, once during this invocation, and again when needing to cancel the 
   * futures.  Because of that it is critical the {@link Iterable} provided returns the exact same 
   * future contents at the time of invoking this call.  If that guarantee can not be provided, 
   * you must specify {@code true} for the {@code copy} parameter.
   * 
   * @since 4.7.2
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
      callbackFutures = futuresCopy = new ArrayList<>();
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
   * <p>
   * If {@code null} is provided here the static instance of 
   * {@link ImmediateResultListenableFuture#NULL_RESULT} will be returned to reduce GC overhead.
   * 
   * @since 1.2.0
   * @param <T> The result object type returned by the returned future
   * @param result result to be provided in .get() call
   * @return Already satisfied future
   */
  @SuppressWarnings("unchecked")
  public static <T> ListenableFuture<T> immediateResultFuture(T result) {
    if (result == null) {
      return (ListenableFuture<T>)ImmediateResultListenableFuture.NULL_RESULT;
    } else {
      return new ImmediateResultListenableFuture<>(result);
    }
  }
  
  /**
   * Constructs a {@link ListenableFuture} that has failed with the given failure.  Thus the 
   * resulting future can not block, or be canceled.  Calls to {@link ListenableFuture#get()} will 
   * immediately throw an {@link ExecutionException}.
   * 
   * @since 1.2.0
   * @param <T> The result object type returned by the returned future
   * @param failure to provide as cause for ExecutionException thrown from .get() call
   * @return Already satisfied future
   */
  public static <T> ListenableFuture<T> immediateFailureFuture(Throwable failure) {
    return new ImmediateFailureListenableFuture<>(failure);
  }

  /**
   * Will continue to schedule the provided task as long as the task is returning a {@code null} 
   * result.  This can be a good way to implement retry logic where a result ultimately needs to be 
   * communicated through a future.  
   * <p>
   * The returned future will only complete with a result once the provided task returns a 
   * non-null result.  Canceling the returned future will prevent future executions from being 
   * attempted.  Canceling with an interrupt will transmit the interrupt to the running task if it 
   * is currently running.  
   * <p>
   * The first execution will either be immediately executed in thread or submitted for immediate 
   * execution on the provided scheduler (depending on {@code firstRunAsync} parameter).  Once 
   * this execution completes, if the result is {@code null} then the task will be rescheduled for 
   * execution.  If non-null then the result will be able to be retrieved from the returned 
   * {@link ListenableFuture}.  
   * <p>
   * If you want to ensure this does not reschedule forever consider using 
   * {@link #scheduleWhileTaskResultNull(SubmitterScheduler, long, boolean, Callable, long)}.
   * 
   * @since 5.0
   * @param <T> The result object type returned by the task and provided by the future
   * @param scheduler Scheduler to schedule out task executions
   * @param scheduleDelayMillis Delay in milliseconds to schedule out future attempts
   * @param firstRunAsync {@code False} to run first try on invoking thread, {@code true} to submit on scheduler
   * @param task Task which will provide result, or {@code null} to reschedule itself again
   * @return Future that will resolve with non-null result from task
   */
  public static <T> ListenableFuture<T> scheduleWhileTaskResultNull(SubmitterScheduler scheduler, 
                                                                    long scheduleDelayMillis, 
                                                                    boolean firstRunAsync, 
                                                                    Callable<? extends T> task) {
    return scheduleWhileTaskResultNull(scheduler, scheduleDelayMillis, firstRunAsync, task, -1);
  }

  /**
   * Will continue to schedule the provided task as long as the task is returning a {@code null} 
   * result.  This can be a good way to implement retry logic where a result ultimately needs to be 
   * communicated through a future.  
   * <p>
   * The returned future will only complete with a result once the provided task returns a non-null 
   * result, or until the provided timeout is reached.  If the timeout is reached then the task 
   * wont be rescheduled.  Instead the future will be resolved with a {@code null} result.  Even if 
   * only 1 millisecond before timeout, the entire {@code rescheduleDelayMillis} will be provided 
   * for the next attempt's scheduled delay.  Canceling the returned future will prevent future 
   * executions from being attempted.  Canceling with an interrupt will transmit the interrupt to 
   * the running task if it is currently running.  
   * <p>
   * The first execution will either be immediately executed in thread or submitted for immediate 
   * execution on the provided scheduler (depending on {@code firstRunAsync} parameter).  Once 
   * this execution completes, if the result is {@code null} then the task will be rescheduled for 
   * execution.  If non-null then the result will be able to be retrieved from the returned 
   * {@link ListenableFuture}.
   * 
   * @since 5.0
   * @param <T> The result object type returned by the task and provided by the future
   * @param scheduler Scheduler to schedule out task executions
   * @param scheduleDelayMillis Delay in milliseconds to schedule out future attempts
   * @param firstRunAsync {@code False} to run first try on invoking thread, {@code true} to submit on scheduler
   * @param task Task which will provide result, or {@code null} to reschedule itself again
   * @param timeoutMillis Timeout in milliseconds task wont be rescheduled and instead just finish with {@code null}
   * @return Future that will resolve with non-null result from task
   */
  public static <T> ListenableFuture<T> scheduleWhileTaskResultNull(SubmitterScheduler scheduler, 
                                                                    long scheduleDelayMillis, 
                                                                    boolean firstRunAsync, 
                                                                    Callable<? extends T> task, 
                                                                    long timeoutMillis) {
    return scheduleWhile(scheduler, scheduleDelayMillis, firstRunAsync, task, 
                         (r) -> r == null, timeoutMillis, true);
  }
  
  /**
   * Executes a task, checking the result from the task to see if it needs to reschedule the task 
   * again.  This can be a good way to implement retry logic where a result ultimately needs to be 
   * communicated through a future.  
   * <p>
   * The returned future will only provide a result once the looping of the task has completed.  
   * Canceling the returned future will prevent future executions from being attempted.  Canceling 
   * with an interrupt will transmit the interrupt to the running task if it is currently running.  
   * <p>
   * The first execution will either be immediately executed in thread or submitted for immediate 
   * execution on the provided scheduler (depending on {@code firstRunAsync} parameter).  Once 
   * this execution completes the result will be provided to the {@link Predicate} to determine if 
   * another schedule should occur to re-run the task.  
   * <p>
   * If you want to ensure this does not reschedule forever consider using 
   * {@link #scheduleWhile(SubmitterScheduler, long, boolean, Callable, Predicate, long, boolean)}.
   *  
   * @since 5.0
   * @param <T> The result object type returned by the task and provided by the future
   * @param scheduler Scheduler to schedule out task executions
   * @param scheduleDelayMillis Delay after predicate indicating to loop again before re-executed
   * @param firstRunAsync {@code False} to run first try on invoking thread, {@code true} to submit on scheduler
   * @param task Task which will provide result to compare in provided {@code Predicate}
   * @param loopTest Test for result to see if scheduled loop should continue
   * @return Future that will resolve once returned {@link Predicate} returns {@code false}
   */
  public static <T> ListenableFuture<T> scheduleWhile(SubmitterScheduler scheduler, 
                                                      long scheduleDelayMillis, 
                                                      boolean firstRunAsync, 
                                                      Callable<? extends T> task, 
                                                      Predicate<? super T> loopTest) {
    return scheduleWhile(scheduler, scheduleDelayMillis, firstRunAsync, task, loopTest, -1, false);
  }

  /**
   * Executes a task, checking the result from the task to see if it needs to reschedule the task 
   * again.  This can be a good way to implement retry logic where a result ultimately needs to be 
   * communicated through a future.  
   * <p>
   * The returned future will only provide a result once the looping of the task has completed, or 
   * until the provided timeout is reached.  If the timeout is reached then the task wont be 
   * rescheduled.  Even if only 1 millisecond before timeout, the entire 
   * {@code rescheduleDelayMillis} will be provided for the next attempt's scheduled delay.  On a 
   * timeout, if {@code true} was provided for {@code timeoutProvideLastValue} then the future 
   * will be resolved with the last result provided.  If {@code false} was provided then the 
   * future will complete in an error state, the cause of which being a {@link TimeoutException}.
   * Canceling the returned future will prevent future executions from being attempted.  Canceling 
   * with an interrupt will transmit the interrupt to the running task if it is currently running.  
   * <p>
   * The first execution will either be immediately executed in thread or submitted for immediate 
   * execution on the provided scheduler (depending on {@code firstRunAsync} parameter).  Once 
   * this execution completes the result will be provided to the {@link Predicate} to determine if 
   * another schedule should occur to re-run the task.  
   *  
   * @since 5.0
   * @param <T> The result object type returned by the task and provided by the future
   * @param scheduler Scheduler to schedule out task executions
   * @param scheduleDelayMillis Delay after predicate indicating to loop again before re-executed
   * @param firstRunAsync {@code False} to run first try on invoking thread, {@code true} to submit on scheduler
   * @param task Task which will provide result to compare in provided {@code Predicate}
   * @param loopTest Test for result to see if scheduled loop should continue
   * @param timeoutMillis If greater than zero, wont reschedule and instead will just return the last result
   * @param timeoutProvideLastValue On timeout {@code false} will complete with a TimeoutException, 
   *                                  {@code true} completes with the last result
   * @return Future that will resolve once returned {@link Predicate} returns {@code false} or timeout is reached
   */
  public static <T> ListenableFuture<T> scheduleWhile(SubmitterScheduler scheduler, 
                                                      long scheduleDelayMillis, 
                                                      boolean firstRunAsync, 
                                                      Callable<? extends T> task, 
                                                      Predicate<? super T> loopTest, 
                                                      long timeoutMillis, 
                                                      boolean timeoutProvideLastValue) {
    return scheduleWhile(scheduler, scheduleDelayMillis, 
                         firstRunAsync ? 
                           scheduler.submit(task) : 
                           SameThreadSubmitterExecutor.instance().submit(task), 
                         task, loopTest, timeoutMillis, timeoutProvideLastValue);
  }
  
  /**
   * Executes a task, checking the result from the task to see if it needs to reschedule the task 
   * again.  This can be a good way to implement retry logic where a result ultimately needs to be 
   * communicated through a future.  
   * <p>
   * The returned future will only provide a result once the looping of the task has completed.  
   * Canceling the returned future will prevent future executions from being attempted.  Canceling 
   * with an interrupt will transmit the interrupt to the running task if it is currently running.  
   * <p>
   * The first execution will happen as soon as the provided {@code startingFuture} completes.  
   * <p>
   * If you want to ensure this does not reschedule forever consider using 
   * {@link #scheduleWhile(SubmitterScheduler, long, ListenableFuture, Callable, Predicate, long, boolean)}.
   *  
   * @since 5.0
   * @param <T> The result object type returned by the task and provided by the future
   * @param scheduler Scheduler to schedule out task executions
   * @param scheduleDelayMillis Delay after predicate indicating to loop again before re-executed
   * @param startingFuture Future to use for first result to test for loop
   * @param task Task which will provide result to compare in provided {@code Predicate}
   * @param loopTest Test for result to see if scheduled loop should continue
   * @return Future that will resolve once returned {@link Predicate} returns {@code false}
   */
  public static <T> ListenableFuture<T> scheduleWhile(SubmitterScheduler scheduler, 
                                                      long scheduleDelayMillis, 
                                                      ListenableFuture<? extends T> startingFuture, 
                                                      Callable<? extends T> task, 
                                                      Predicate<? super T> loopTest) {
    return scheduleWhile(scheduler, scheduleDelayMillis, startingFuture, task, loopTest, -1, false);
  }

  /**
   * Executes a task, checking the result from the task to see if it needs to reschedule the task 
   * again.  This can be a good way to implement retry logic where a result ultimately needs to be 
   * communicated through a future.  
   * <p>
   * The returned future will only provide a result once the looping of the task has completed, or 
   * until the provided timeout is reached.  If the timeout is reached then the task wont be 
   * rescheduled.  Even if only 1 millisecond before timeout, the entire 
   * {@code rescheduleDelayMillis} will be provided for the next attempt's scheduled delay.  On a 
   * timeout, if {@code true} was provided for {@code timeoutProvideLastValue} then the future 
   * will be resolved with the last result provided.  If {@code false} was provided then the 
   * future will complete in an error state, the cause of which being a {@link TimeoutException}.
   * Canceling the returned future will prevent future executions from being attempted.  Canceling 
   * with an interrupt will transmit the interrupt to the running task if it is currently running.  
   * <p>
   * The first execution will happen as soon as the provided {@code startingFuture} completes.  
   *  
   * @since 5.0
   * @param <T> The result object type returned by the task and provided by the future
   * @param scheduler Scheduler to schedule out task executions
   * @param scheduleDelayMillis Delay after predicate indicating to loop again before re-executed
   * @param startingFuture Future to use for first result to test for loop
   * @param task Task which will provide result to compare in provided {@code Predicate}
   * @param loopTest Test for result to see if scheduled loop should continue
   * @param timeoutMillis If greater than zero, wont reschedule and instead will just return the last result
   * @param timeoutProvideLastValue On timeout {@code false} will complete with a TimeoutException, 
   *                                  {@code true} completes with the last result
   * @return Future that will resolve once returned {@link Predicate} returns {@code false}
   */
  public static <T> ListenableFuture<T> scheduleWhile(SubmitterScheduler scheduler, 
                                                      long scheduleDelayMillis, 
                                                      ListenableFuture<? extends T> startingFuture, 
                                                      Callable<? extends T> task, 
                                                      Predicate<? super T> loopTest, 
                                                      long timeoutMillis, 
                                                      boolean timeoutProvideLastValue) {
    ArgumentVerifier.assertNotNull(scheduler, "scheduler");
    ArgumentVerifier.assertNotNegative(scheduleDelayMillis, "scheduleDelayMillis");
    ArgumentVerifier.assertNotNull(startingFuture, "startingFuture");
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNull(loopTest, "loopTest");
    
    // shortcut optimization in case future is done, and loop is not needed
    if (startingFuture.isDone()) {
      if (startingFuture.isCancelled()) {
        SettableListenableFuture<T> resultFuture = new SettableListenableFuture<>();
        resultFuture.cancel(false);
        return resultFuture;
      }
      try {
        if (! loopTest.test(startingFuture.get())) {
          return immediateResultFuture(startingFuture.get());
        } // else, we need to go through the normal scheduling logic below
      } catch (ExecutionException e) {
        return immediateFailureFuture(e.getCause());
      } catch (InterruptedException e) {
        // should not be possible
        throw new RuntimeException(e);
      } catch (Throwable t) {
        ExceptionUtils.handleException(t);
        return immediateFailureFuture(t);
      }
    }
    
    long startTime = timeoutMillis > 0 ? Clock.accurateForwardProgressingMillis() : -1;
    // can not assert not resolved in parallel because user may cancel future at any point
    SettableListenableFuture<T> resultFuture = new SettableListenableFuture<>(false);
    Callable<T> cancelCheckingTask = new Callable<T>() {
      @Override
      public T call() throws Exception {
        // set thread before check canceled state so if canceled with interrupt we will interrupt the call
        resultFuture.setRunningThread(Thread.currentThread());
        try {
          if (! resultFuture.isCancelled()) {
            return task.call();
          } else {
            // result future is already complete, so throw to avoid executing, but only throw such 
            // that we wont attempt to do anything with the result future
            throw FailurePropogatingFutureCallback.IGNORED_FAILURE;
          }
        } finally {
          resultFuture.setRunningThread(null);
        }
      }
    };
    
    startingFuture.addCallback(new FailurePropogatingFutureCallback<T>(resultFuture) {
      @Override
      public void handleResult(T result) {
        try {
          if (startTime > 0 && Clock.lastKnownForwardProgressingMillis() - startTime > timeoutMillis) {
            if (timeoutProvideLastValue) {
              resultFuture.setResult(result);
            } else {
              resultFuture.setFailure(new TimeoutException());
            }
          } else if (loopTest.test(result)) {
            scheduler.submitScheduled(cancelCheckingTask, scheduleDelayMillis)
                     .addCallback(this);  // add this to check again once execution completes
          } else {
            // once we have our result, this will end our loop
            resultFuture.setResult(result);
          }
        } catch (Throwable t) {
          // failure likely from the predicate test, handle exception 
          // so the behavior is closer to if the exception was thrown from a task submitted to the pool
          ExceptionUtils.handleException(t);
          
          resultFuture.setFailure(t);
        }
      }
    });
    
    return resultFuture;
  }

  /**
   * Executes a task until the provided supplier returns {@code false}.  This can be a good way to 
   * implement retry logic where there completion (but not result) needs to be communicated.  If 
   * a result is needed please see 
   * {@link #scheduleWhile(SubmitterScheduler, long, boolean, Callable, Predicate)}.
   * <p>
   * The returned future will only provide a result once the looping of the task has completed.  
   * Canceling the returned future will prevent future executions from being attempted.  Canceling 
   * with an interrupt will transmit the interrupt to the running task if it is currently running.  
   * <p>
   * The first execution will either be immediately executed in thread or submitted for immediate 
   * execution on the provided scheduler (depending on {@code firstRunAsync} parameter).  Once 
   * this execution completes the result will be provided to the {@link Supplier} to determine if 
   * another schedule should occur to re-run the task.  
   * <p>
   * If you want to ensure this does not reschedule forever consider using 
   * {@link #scheduleWhile(SubmitterScheduler, long, boolean, Runnable, Supplier, long)}.
   *  
   * @since 5.0
   * @param scheduler Scheduler to schedule out task executions
   * @param scheduleDelayMillis Delay after predicate indicating to loop again before re-executed
   * @param firstRunAsync {@code False} to run first try on invoking thread, {@code true} to submit on scheduler
   * @param task Task to execute as long as test returns {@code true}
   * @param loopTest Test to see if scheduled loop should continue
   * @return Future that will resolve once returned {@link Supplier} returns {@code false}
   */
  public static ListenableFuture<?> scheduleWhile(SubmitterScheduler scheduler, 
                                                  long scheduleDelayMillis, 
                                                  boolean firstRunAsync, 
                                                  Runnable task, 
                                                  Supplier<Boolean> loopTest) {
    return scheduleWhile(scheduler, scheduleDelayMillis, firstRunAsync, task, loopTest, -1);
  }

  /**
   * Executes a task, checking the result from the task to see if it needs to reschedule the task 
   * again.  This can be a good way to implement retry logic where a result ultimately needs to be 
   * communicated through a future.  
   * <p>
   * The returned future will only provide a result once the looping of the task has completed, or 
   * until the provided timeout is reached.  If the timeout is reached then the task wont be 
   * rescheduled.  Even if only 1 millisecond before timeout, the entire 
   * {@code rescheduleDelayMillis} will be provided for the next attempt's scheduled delay.  On a 
   * timeout, if {@code true} was provided for {@code timeoutProvideLastValue} then the future 
   * will be resolved with the last result provided.  If {@code false} was provided then the 
   * future will complete in an error state, the cause of which being a {@link TimeoutException}.
   * Canceling the returned future will prevent future executions from being attempted.  Canceling 
   * with an interrupt will transmit the interrupt to the running task if it is currently running.  
   * <p>
   * The first execution will either be immediately executed in thread or submitted for immediate 
   * execution on the provided scheduler (depending on {@code firstRunAsync} parameter).  Once 
   * this execution completes the result will be provided to the {@link Supplier} to determine if 
   * another schedule should occur to re-run the task.  
   *  
   * @since 5.0
   * @param scheduler Scheduler to schedule out task executions
   * @param scheduleDelayMillis Delay after predicate indicating to loop again before re-executed
   * @param firstRunAsync {@code False} to run first try on invoking thread, {@code true} to submit on scheduler
   * @param task Task to execute as long as test returns {@code true}
   * @param loopTest Test to see if scheduled loop should continue
   * @param timeoutMillis If greater than zero, wont reschedule and instead will just return the last result
   * @return Future that will resolve once returned {@link Supplier} returns {@code false}
   */
  public static ListenableFuture<?> scheduleWhile(SubmitterScheduler scheduler, 
                                                  long scheduleDelayMillis, 
                                                  boolean firstRunAsync, 
                                                  Runnable task, 
                                                  Supplier<Boolean> loopTest,
                                                  long timeoutMillis) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNull(loopTest, "loopTest");
    
    return scheduleWhile(scheduler, scheduleDelayMillis, 
                         firstRunAsync, new RunnableCallableAdapter<>(task, null), 
                         (ignored) -> loopTest.get(), timeoutMillis, false);
  }
  
  /**
   * Transform a future's result into another future by applying the provided transformation 
   * function.  If the future completed in error, then the mapper will not be invoked, and instead 
   * the returned future will be completed in the same error state this future resulted in.  If the 
   * mapper function itself throws an Exception, then the returned future will result in the error 
   * thrown from the mapper.  
   * <p>
   * This can be easily used to chain together a series of operations, happening async until the 
   * final result is actually needed.  Once the future completes the mapper function will be invoked 
   * on the executor (if provided).  Because of that providing an executor can ensure this will 
   * never block.  If an executor is not provided then the mapper may be invoked on the calling 
   * thread (if the future is already complete), or on the same thread which the future completes 
   * on.  If the mapper function is very fast and cheap to run then {@link #map(Function)} or 
   * providing {@code null} for the executor can allow more efficient operation.
   * 
   * @since 5.0
   * @param <ST> The source type for the object returned from the future and inputed into the mapper
   * @param <RT> The result type for the object returned from the mapper
   * @param sourceFuture Future to source input into transformation function
   * @param transformer Function to apply result from future into returned future
   * @param executor Executor to execute transformation function on, or {@code null}
   * @return Future with result of transformation function or respective error
   */
  protected static <ST, RT> ListenableFuture<RT> transform(ListenableFuture<ST> sourceFuture, 
                                                           Function<? super ST, ? extends RT> transformer, 
                                                           Executor executor, 
                                                           ListenerOptimizationStrategy optimizeExecution) {
    if (executor == null & sourceFuture.isDone() && ! sourceFuture.isCancelled()) {
      // optimized path for already complete futures which we can now process in thread
      try {
        return FutureUtils.immediateResultFuture(transformer.apply(sourceFuture.get()));
      } catch (InterruptedException e) {
        // should not be possible
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        // failure in getting result from future, transfer failure
        return FutureUtils.immediateFailureFuture(e.getCause());
      } catch (Throwable t) {
        // failure calculating transformation, let handler get a chance to see the uncaught exception
        // This makes the behavior closer to if the exception was thrown from a task submitted to the pool
        ExceptionUtils.handleException(t);
        
        return FutureUtils.immediateFailureFuture(t);
      }
    } else if (sourceFuture.isCancelled()) { // shortcut to avoid exception generation
      SettableListenableFuture<RT> slf = new SettableListenableFuture<>();
      slf.cancel(false);
      return slf;
    } else {
      SettableListenableFuture<RT> slf = 
          new CancelDelegateSettableListenableFuture<>(sourceFuture, executor);
      // may still process in thread if future completed after check and executor is null
      sourceFuture.addCallback(new FailurePropogatingFutureCallback<ST>(slf) {
        @Override
        public void handleResult(ST result) {
          try {
            slf.setRunningThread(Thread.currentThread());
            slf.setResult(transformer.apply(result));
          } catch (Throwable t) {
            // failure calculating transformation, let handler get a chance to see the uncaught exception
            // This makes the behavior closer to if the exception was thrown from a task submitted to the pool
            ExceptionUtils.handleException(t);
            
            slf.setFailure(t);
          }
        }
      }, executor, optimizeExecution);
      return slf;
    }
  }
  
  /**
   * Similar to {@link #transform(ListenableFuture, Function, Executor)} except designed to handle 
   * functions which return futures.  This will take what otherwise would be 
   * {@code ListenableFuture<ListenableFuture<R>>}, and flattens it into a single future which will 
   * resolve once the contained future is complete.
   * 
   * @since 5.0
   * @param <ST> The source type for the object returned from the future and inputed into the mapper
   * @param <RT> The result type for the object contained in the future returned from the mapper
   * @param sourceFuture Future to source input into transformation function
   * @param transformer Function to apply result from future into returned future
   * @param executor Executor to execute transformation function on, or {@code null}
   * @return Future with result of transformation function or respective error
   */
  protected static <ST, RT> ListenableFuture<RT> flatTransform(ListenableFuture<ST> sourceFuture, 
                                                               Function<? super ST, ListenableFuture<RT>> transformer, 
                                                               Executor executor, 
                                                               ListenerOptimizationStrategy optimizeExecution) {
    if (executor == null & sourceFuture.isDone() && ! sourceFuture.isCancelled()) {
      // optimized path for already complete futures which we can now process in thread
      try {
        return transformer.apply(sourceFuture.get());
      } catch (InterruptedException e) {
        // should not be possible
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        // failure in getting result from future, transfer failure
        return FutureUtils.immediateFailureFuture(e.getCause());
      } catch (Throwable t) {
        // failure calculating transformation, let handler get a chance to see the uncaught exception
        // This makes the behavior closer to if the exception was thrown from a task submitted to the pool
        ExceptionUtils.handleException(t);
        
        return FutureUtils.immediateFailureFuture(t);
      }
    } else if (sourceFuture.isCancelled()) { // shortcut to avoid exception generation
      SettableListenableFuture<RT> slf = new SettableListenableFuture<>();
      slf.cancel(false);
      return slf;
    } else {
      SettableListenableFuture<RT> slf = 
          new CancelDelegateSettableListenableFuture<>(sourceFuture, executor);
      sourceFuture.addCallback(new FailurePropogatingFutureCallback<ST>(slf) {
        @Override
        public void handleResult(ST result) {
          try {
            slf.setRunningThread(Thread.currentThread());
            transformer.apply(result).addCallback(new FailurePropogatingFutureCallback<RT>(slf) {
              @Override
              public void handleResult(RT result) {
                slf.setResult(result);
              }
            });
            slf.setRunningThread(null); // may be processing async now
          } catch (Throwable t) {
            // failure calculating transformation, let handler get a chance to see the uncaught exception
            // This makes the behavior closer to if the exception was thrown from a task submitted to the pool
            ExceptionUtils.handleException(t);
            
            slf.setFailure(t);
          }
        }
      }, executor, optimizeExecution);
      return slf;
    }
  }
  
  /**
   * Class which will propagate a failure condition to a {@link SettableListenableFuture} from a 
   * source future which this is added as a {@link FutureCallback} to.
   * 
   * @since 5.0
   * @param <T> Type of result to be accepted by {@link FutureCallback}
   */
  protected abstract static class FailurePropogatingFutureCallback<T> implements FutureCallback<T> {
    /**
     * The instance of the only exception which this callback will not propagate.  It must be the 
     * exact exception, and can not be hidden inside a cause chain.
     */
    protected static final RuntimeException IGNORED_FAILURE = new SuppressedStackRuntimeException();
    
    private final SettableListenableFuture<?> settableFuture;
    
    protected FailurePropogatingFutureCallback(SettableListenableFuture<?> settableFuture) {
      this.settableFuture = settableFuture;
    }
    
    @Override
    public void handleFailure(Throwable t) {
      if (t == IGNORED_FAILURE) {
        // ignored
      } else if (t instanceof CancellationException) {
        settableFuture.cancel(false);
      } else {
        settableFuture.setFailure(t);
      }
    }
  }

  /**
   * Implementation of {@link SettableListenableFuture} which delegates it's cancel operation to a 
   * parent future.
   * 
   * @since 4.1.0
   * @param <T> The result object type returned from the futures
   */
  protected static class CancelDelegateSettableListenableFuture<T> extends SettableListenableFuture<T> {
    private final ListenableFuture<?> cancelDelegateFuture;
    
    protected CancelDelegateSettableListenableFuture(ListenableFuture<?> lf) {
      this(lf, null);
    }
    
    protected CancelDelegateSettableListenableFuture(ListenableFuture<?> lf, 
                                                     Executor executingExecutor) {
      super(false, executingExecutor);
      cancelDelegateFuture = lf;
    }
    
    protected boolean cancelRegardlessOfDelegateFutureState(boolean interruptThread) {
      if (super.cancel(interruptThread)) {
        cancelDelegateFuture.cancel(interruptThread);
        return true;
      } else {
        return false;
      }
    }

    @Override
    public boolean cancel(boolean interruptThread) {
      if (interruptThread) {
        // if we want to interrupt, we want to try to cancel ourselves even if our delegate has 
        // already completed (in case there is processing associated to this future we can avoid)
        return cancelRegardlessOfDelegateFutureState(true);
      } else {
        if (cancelDelegateFuture.cancel(false)) {
          return super.cancel(false);
        } else {
          return false;
        }
      }
    }
  }
  
  /**
   * A future implementation that will return a List of futures as the result.  The future will 
   * not be satisfied till all provided futures have completed.
   * 
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
      buildingResult = new AtomicReference<>(null);
      futures = new ArrayList<>();
      
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
     * <p>
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
        
        ConcurrentArrayList<ListenableFuture<? extends T>> newList = 
            new ConcurrentArrayList<>(0, rearPadding);
        
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
     * Task which is ran after a future completes.  This is used internally to track how many 
     * outstanding tasks are remaining, as well as used to collect the results if desired.
     * 
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
   * A future implementation that will be satisfied till all provided futures have completed.
   * 
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
   * A future implementation that will return a List of futures as the result.  The future will 
   * not be satisfied till all provided futures have completed.
   * <p>
   * This implementation will return a result of all the futures that completed.
   * 
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
   * A future implementation that will return a List of futures as the result.  The future will 
   * not be satisfied till all provided futures have completed.
   * <p>
   * This implementation will return a result of all the futures that completed successfully.  
   * If the future was canceled or threw an exception it will not be included.
   * 
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
   *  future implementation that will return a List of futures as the result.  The future will 
   * not be satisfied till all provided futures have completed.
   * <p>
   * This implementation will return a result of all the futures that either threw an exception 
   * during computation, or was canceled.
   * 
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
   * Future callback that on error condition will cancel all the provided futures.
   * 
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
