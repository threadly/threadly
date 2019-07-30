package org.threadly.concurrent.future;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ArrayIterator;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;

/**
 * A collection of small utilities for handling futures.  This class has lots of tools for dealing 
 * with collections of futures, ranging from blocking, extracting results, and more.
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
 * <li>{@link #invokeAfterAllComplete(Collection, Runnable)}
 * <li>{@link #invokeAfterAllComplete(Collection, Runnable, Executor)}
 * <li>{@link #makeFirstResultFuture(Collection, boolean, boolean)}
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
 * <li>{@link #scheduleWhile(SubmitterScheduler, long, boolean, Callable, Predicate)}
 * <li>{@link #scheduleWhile(SubmitterScheduler, long, boolean, Callable, Predicate, long, boolean)}
 * <li>{@link #scheduleWhile(SubmitterScheduler, long, ListenableFuture, Callable, Predicate)}
 * <li>{@link #scheduleWhile(SubmitterScheduler, long, ListenableFuture, Callable, Predicate, long, boolean)}
 * <li>{@link #scheduleWhile(SubmitterScheduler, long, boolean, Runnable, Supplier)}
 * <li>{@link #scheduleWhile(SubmitterScheduler, long, boolean, Runnable, Supplier, long)}
 * <li>{@link #executeWhile(Callable, Predicate)}
 * <li>{@link #executeWhile(Callable, Predicate, long, boolean)}
 * <li>{@link #executeWhile(ListenableFuture, Callable, Predicate)}
 * <li>{@link #executeWhile(ListenableFuture, Callable, Predicate, long, boolean)}
 * </ul>
 * 
 * @since 1.0.0
 */
public class FutureUtils extends InternalFutureUtils {
  /**
   * This call blocks till all futures in the list have completed.  If the future completed with 
   * an error, the {@link ExecutionException} is swallowed.  Meaning that this does not attempt to 
   * verify that all futures completed successfully.  If you need to know if any failed, please 
   * use {@link #blockTillAllCompleteOrFirstError(Iterable)}.  
   * <p>
   * If you need to specify a timeout to control how long to block, consider using 
   * {@link #blockTillAllComplete(Iterable, long)}.
   * 
   * @param futures Futures to block till they complete
   * @throws InterruptedException Thrown if thread is interrupted while waiting on future
   */
  public static void blockTillAllComplete(Future<?> ... futures) throws InterruptedException {
    countFuturesWithResult(ArrayIterator.makeIterable(futures), null);
  }
  
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
   * @param futures Futures to iterate over
   * @throws InterruptedException Thrown if thread is interrupted while waiting on future
   * @throws ExecutionException Thrown if future throws exception on .get() call
   */
  public static void blockTillAllCompleteOrFirstError(Future<?> ... futures) 
      throws InterruptedException, ExecutionException {
    blockTillAllCompleteOrFirstError(ArrayIterator.makeIterable(futures));
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
    for (Future<?> f : futures) {
      f.get();
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
  public static void blockTillAllCompleteOrFirstError(Iterable<? extends Future<?>> futures, 
                                                      long timeoutInMillis) 
      throws InterruptedException, TimeoutException, ExecutionException {
    if (futures == null) {
      return;
    }
    long startTime = Clock.accurateForwardProgressingMillis();
    long remainingTime;
    for (Future<?> f : futures) {
      if ((remainingTime = timeoutInMillis - (Clock.lastKnownForwardProgressingMillis() - startTime)) <= 0) {
        throw new TimeoutException();
      }
      f.get(remainingTime, TimeUnit.MILLISECONDS);
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
    for (Future<?> f : futures) {
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
      } catch (CancellationException | ExecutionException e) {
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
    long startTime = Clock.accurateForwardProgressingMillis();
    long remainingTime;
    for (Future<?> f : futures) {
      if ((remainingTime = timeoutInMillis - (Clock.lastKnownForwardProgressingMillis() - startTime)) <= 0) {
        throw new TimeoutException();
      }
      try {
        if (comparisonResult == null) {
          if (f.get(remainingTime, TimeUnit.MILLISECONDS) == null) {
            resultCount++;
          }
        } else if (comparisonResult.equals(f.get(remainingTime, TimeUnit.MILLISECONDS))) {
          resultCount++;
        }
      } catch (CancellationException | ExecutionException e) {
        // swallowed
      }
    }
    return resultCount;
  }
  
  /**
   * A potentially more performant option than {@link #makeCompleteFuture(List)} when only a 
   * listener invocation is desired after all the futures complete.  This is effective an async 
   * implementation of {@link #blockTillAllComplete(Iterable)}.  If the listener needs to be 
   * invoked on another thread than one of the provided futures please use 
   * {@link #invokeAfterAllComplete(Collection, Runnable, Executor)}.  Please see 
   * {@link ListenableFuture#listener(Runnable)} for more information on execution without an 
   * {@link Executor}.
   * <p>
   * It is critical that the collection is NOT modified while this is invoked.  A change in the 
   * futures contained in the collection will lead to unreliable behavior with the exectuion of the 
   * listener.
   * 
   * @param futures Futures that must complete before listener is invoked
   * @param listener Invoked once all the provided futures have completed
   */
  public static void invokeAfterAllComplete(Collection<? extends ListenableFuture<?>> futures, 
                                            Runnable listener) {
    invokeAfterAllComplete(futures, listener, null);
  }
  
  /**
   * A potentially more performant option than {@link #makeCompleteFuture(List)} when only a 
   * listener invocation is desired after all the futures complete.  This is effective an async 
   * implementation of {@link #blockTillAllComplete(Iterable)}.
   * <p>
   * It is critical that the collection is NOT modified while this is invoked.  A change in the 
   * futures contained in the collection will lead to unreliable behavior with the exectuion of the 
   * listener.
   * 
   * @param futures Futures that must complete before listener is invoked
   * @param listener Invoked once all the provided futures have completed
   * @param executor Executor (or {@code null}) to invoke listener on, see 
   *                    {@link ListenableFuture#listener(Runnable, Executor)}
   */
  public static void invokeAfterAllComplete(Collection<? extends ListenableFuture<?>> futures, 
                                            Runnable listener, Executor executor) {
    ArgumentVerifier.assertNotNull(listener, "listener");
    
    int size = futures == null ? 0 : futures.size();
    if (size == 0) {
      if (executor == null) {
        listener.run();
      } else {
        executor.execute(listener);
      }
    } else if (size == 1) {
      futures.iterator().next().listener(listener, executor);
    } else {
      AtomicInteger remaining = new AtomicInteger(size);
      Runnable decrementingListener = () -> {
        if (remaining.decrementAndGet() == 0) {
          if (executor == null) {
            listener.run();
          } else {
            executor.execute(listener);
          }
        }
      };
      for (ListenableFuture<?> lf : futures) {
        lf.listener(decrementingListener);
      }
    }
  }
  
  /**
   * Converts a collection of {@link ListenableFuture}'s into a single {@link ListenableFuture} 
   * where the result will be the first result provided from the collection.
   * <p>
   * If {@code ignoreErrors} is {@code false} the returned future will complete as soon as the 
   * first future completes, if it completes in error then the error would be returned.  If 
   * {@code ignoreErrors} is {@code true} then the returned future will complete once a result is 
   * provided, or once all futures have completed in error.  If all futures did complete in error 
   * then the last error state will be specified to the resulting {@link ListenableFuture}.  This 
   * minor bookkeeping to ignore errors does incur a slight overhead.
   * <p>
   * It is expected that the first result is the only result desired, once it is found this will 
   * attempt to cancel all remaining futures.  If you may want other results which were in 
   * progress, then specifying {@code interruptOnCancel} as {@code false} will mean that any 
   * futures which started can complete.  You can then inspect the collection for done futures 
   * which might have a result.  If there is no concern for other results, then you likely will 
   * want to interrupt started futures.
   * 
   * @since 5.38
   * @param <T> type of result provided in the returned future
   * @param c Collection of futures to monitor for result
   * @param ignoreErrors {@code false} to communicate the first completed future state, even if in error
   * @param interruptOnCancel {@code true} to send a interrupt on any running futures after we have a result
   * @return A future which will be provided the first result from any in the provided {@link Collection}
   */
  public static <T> ListenableFuture<T> makeFirstResultFuture(Collection<? extends ListenableFuture<? extends T>> c, 
                                                              boolean ignoreErrors, boolean interruptOnCancel) {
    SettableListenableFuture<T> result = new SettableListenableFuture<>(false);
    FutureCallback<T> callback;
    if (ignoreErrors) {
      AtomicInteger errorsRemaining = new AtomicInteger(c.size());
      callback = new FutureCallback<T>() {
        @Override
        public void handleResult(T t) {
          if (result.setResult(t)) {
            FutureUtils.cancelIncompleteFutures(c, interruptOnCancel);
          }
        }

        @Override
        public void handleFailure(Throwable t) {
          if (errorsRemaining.decrementAndGet() == 0) {
            // ignore failures till we reach the last failure
            result.setFailure(t);
          }
        }
      };
    } else {
      callback = new FutureCallback<T>() {
        @Override
        public void handleResult(T t) {
          if (result.setResult(t)) {
            FutureUtils.cancelIncompleteFutures(c, interruptOnCancel);
          }
        }

        @Override
        public void handleFailure(Throwable t) {
          if (result.setFailure(t)) {
            FutureUtils.cancelIncompleteFutures(c, interruptOnCancel);
          }
        }
      };
    }
    
    c.forEach((lf) -> lf.callback(callback));

    return result;
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
   * @since 5.3
   * @param futures Collection of futures that must finish before returned future is satisfied
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static ListenableFuture<?> makeCompleteFuture(Collection<? extends ListenableFuture<?>> futures) {
    if (futures == null || futures.isEmpty()) {
      return ImmediateResultListenableFuture.NULL_RESULT;
    } else if (futures.size() == 1) {
      return futures.iterator().next();
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
   * @param futures Futures that must finish before returned future is satisfied
   * @return ListenableFuture which will be done once all futures provided are done
   */
  public static ListenableFuture<?> makeCompleteFuture(ListenableFuture<?> ... futures) {
    return makeCompleteFuture(ArrayIterator.makeIterable(futures));
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
    if (futures == null) {
      return ImmediateResultListenableFuture.NULL_RESULT;
    }
    Iterator<? extends ListenableFuture<?>> it = futures.iterator();
    if (! it.hasNext()) {
      return ImmediateResultListenableFuture.NULL_RESULT;
    }
    ListenableFuture<?> result = new EmptyFutureCollection(it);
    if (result.isDone()) {
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
  @SuppressWarnings("unchecked")
  public static <T> ListenableFuture<T> makeCompleteFuture(Iterable<? extends ListenableFuture<?>> futures, 
                                                           final T result) {
    if (futures == null) {
      return immediateResultFuture(result);
    }
    Iterator<? extends ListenableFuture<?>> it = futures.iterator();
    if (! it.hasNext()) {
      return immediateResultFuture(result);
    }
    final EmptyFutureCollection efc = new EmptyFutureCollection(it);
    if (efc.isDone()) {
      return immediateResultFuture(result);
    }
    
    final SettableListenableFuture<T> resultFuture = 
        new CancelDelegateSettableListenableFuture<>(efc, null);
    efc.callback(new FutureCallback<Object>() {
      @Override
      public void handleResult(Object ignored) {
        resultFuture.setResult(result);
      }

      @Override
      public void handleFailure(Throwable t) {
        resultFuture.setFailure(t);
      }
    }, null, null);
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
      makeFailurePropagatingCompleteFuture(ListenableFuture<?> ... futures) {
    return makeFailurePropagatingCompleteFuture(ArrayIterator.makeIterable(futures), null);
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
    if (futures == null) {
      return immediateResultFuture(result);
    }
    Iterator<? extends ListenableFuture<?>> it = futures.iterator();
    if (! it.hasNext()) {
      return immediateResultFuture(result);
    }
    FailureFutureCollection<Object> ffc = new FailureFutureCollection<>(it);
    if (ffc.isDone()) {
      // optimize already done case
      try {
        List<ListenableFuture<?>> failedFutures = ffc.get();
        if (failedFutures.isEmpty()) {
          return immediateResultFuture(result);
        } else {
          // propagate error
          ListenableFuture<?> f = failedFutures.get(0);
          if (f.isCancelled()) {
            return new ImmediateCanceledListenableFuture<>(null);
          } else {
            f.get();  // will throw ExecutionException to be handled below
          }
        }
      } catch (ExecutionException e) {
        return immediateFailureFuture(e.getCause());
      } catch (InterruptedException e) {  // should not be possible
        throw new RuntimeException(e);
      }
    }
    
    final CancelDelegateSettableListenableFuture<T> resultFuture = 
        new CancelDelegateSettableListenableFuture<>(ffc, null);
    ffc.callback(new FutureCallback<List<ListenableFuture<?>>>() {
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
            } catch (InterruptedException e) {  // should not be possible
              throw new RuntimeException(e);
            }
          }
        }
      }

      @Override
      public void handleFailure(Throwable t) {
        resultFuture.setFailure(t);
      }
    }, null, null);
    return resultFuture;
  }
  
  /**
   * This call is similar to {@link #makeCompleteFuture(Iterable)} in that it will immediately 
   * provide a future that will not be satisfied till all provided futures complete.  
   * <p>
   * This future provides a list of the completed futures as the result.  The order of the result 
   * list will match the order returned by the provided {@link Iterable}.
   * <p>
   * If {@link ListenableFuture#cancel(boolean)} is invoked on the returned future, all provided 
   * futures will attempt to be canceled in the same way.
   * 
   * @since 1.2.0
   * @param <T> The result object type returned from the futures
   * @param futures Structure of futures to iterate over
   * @return ListenableFuture which will be done once all futures provided are done
   */
  @SuppressWarnings("unchecked")
  public static <T> ListenableFuture<List<ListenableFuture<? extends T>>> 
      makeCompleteListFuture(Iterable<? extends ListenableFuture<? extends T>> futures) {
    if (futures == null) {
      return (ListenableFuture<List<ListenableFuture<? extends T>>>)ImmediateResultListenableFuture.EMPTY_LIST_RESULT;
    }
    Iterator<? extends ListenableFuture<? extends T>> it = futures.iterator();
    if (! it.hasNext()) {
      return (ListenableFuture<List<ListenableFuture<? extends T>>>)ImmediateResultListenableFuture.EMPTY_LIST_RESULT;
    }
    return new AllFutureCollection<>(it);
  }
  
  /**
   * This call is similar to {@link #makeCompleteFuture(Iterable)} in that it will immediately 
   * provide a future that will not be satisfied till all provided futures complete.  
   * <p>
   * This future provides a list of the futures that completed without throwing an exception nor 
   * were canceled.  The order of the resulting list is NOT deterministic.  If order is needed 
   * please see {@link #makeCompleteListFuture(Iterable)} and check for results.
   * <p>
   * If {@link ListenableFuture#cancel(boolean)} is invoked on the returned future, all provided 
   * futures will attempt to be canceled in the same way.
   * 
   * @since 1.2.0
   * @param <T> The result object type returned from the futures
   * @param futures Structure of futures to iterate over
   * @return ListenableFuture which will be done once all futures provided are done
   */
  @SuppressWarnings("unchecked")
  public static <T> ListenableFuture<List<ListenableFuture<? extends T>>> 
      makeSuccessListFuture(Iterable<? extends ListenableFuture<? extends T>> futures) {
    if (futures == null) {
      return (ListenableFuture<List<ListenableFuture<? extends T>>>)ImmediateResultListenableFuture.EMPTY_LIST_RESULT;
    }
    Iterator<? extends ListenableFuture<? extends T>> it = futures.iterator();
    if (! it.hasNext()) {
      return (ListenableFuture<List<ListenableFuture<? extends T>>>)ImmediateResultListenableFuture.EMPTY_LIST_RESULT;
    }
    return new SuccessFutureCollection<>(it);
  }
  
  /**
   * This call is similar to {@link #makeCompleteFuture(Iterable)} in that it will immediately 
   * provide a future that will not be satisfied till all provided futures complete.  
   * <p>
   * This future provides a list of the futures that failed by either throwing an exception or 
   * were canceled.  The order of the resulting list is NOT deterministic.  If order is needed 
   * please see {@link #makeCompleteListFuture(Iterable)} and check for results.
   * <p>
   * If {@link ListenableFuture#cancel(boolean)} is invoked on the returned future, all provided 
   * futures will attempt to be canceled in the same way.
   * 
   * @since 1.2.0
   * @param <T> The result object type returned from the futures
   * @param futures Structure of futures to iterate over
   * @return ListenableFuture which will be done once all futures provided are done
   */
  @SuppressWarnings("unchecked")
  public static <T> ListenableFuture<List<ListenableFuture<? extends T>>> 
      makeFailureListFuture(Iterable<? extends ListenableFuture<? extends T>> futures) {
    if (futures == null) {
      return (ListenableFuture<List<ListenableFuture<? extends T>>>)ImmediateResultListenableFuture.EMPTY_LIST_RESULT;
    }
    Iterator<? extends ListenableFuture<? extends T>> it = futures.iterator();
    if (! it.hasNext()) {
      return (ListenableFuture<List<ListenableFuture<? extends T>>>)ImmediateResultListenableFuture.EMPTY_LIST_RESULT;
    }
    return new FailureFutureCollection<>(it);
  }
  
  /**
   * This returns a future which provides the results of all the provided futures.  Thus 
   * preventing the need to iterate over all the futures and manually extract the results.  This 
   * call does NOT block, instead it will return a future which will not complete until all the 
   * provided futures complete.  
   * <p>
   * The order of the result list will match the order returned by the provided {@link Iterable}.
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
  @SuppressWarnings("unchecked")
  public static <T> ListenableFuture<List<T>> 
      makeResultListFuture(Iterable<? extends ListenableFuture<? extends T>> futures, 
                           final boolean ignoreFailedFutures) {
    if (futures == null) {
      return (ListenableFuture<List<T>>)ImmediateResultListenableFuture.EMPTY_LIST_RESULT;
    }
    Iterator<? extends ListenableFuture<? extends T>> it = futures.iterator();
    if (! it.hasNext()) {
      return (ListenableFuture<List<T>>)ImmediateResultListenableFuture.EMPTY_LIST_RESULT;
    }
    ListenableFuture<List<ListenableFuture<? extends T>>> completeFuture = 
        new AllFutureCollection<>(it);
    final SettableListenableFuture<List<T>> result = 
        new CancelDelegateSettableListenableFuture<>(completeFuture, null);
    
    completeFuture.callback(new FutureCallback<List<ListenableFuture<? extends T>>>() {
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
        result.setFailure(t);
      }
    }, null, null);
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
    Consumer<Throwable> cancelingCallback = 
        new CancelOnErrorFutureCallback(callbackFutures, interruptThread);
    for (ListenableFuture<?> f : futures) {
      if (copy) {
        futuresCopy.add(f);
      }
      f.failureCallback(cancelingCallback);
    }
  }
  
  /**
   * Constructs a {@link ListenableFuture} that has already had the provided result given to it.  
   * Thus the resulting future can not error, block, or be canceled.  
   * <p>
   * If {@code null} is provided here the static instance of 
   * {@link ImmediateResultListenableFuture#NULL_RESULT} will be returned to reduce GC overhead.  
   * This function may additionally try to optimize the references to other common cases (like 
   * {@link Boolean} results) when performance permits it.  Those de-duplications may change based 
   * off benchmarking results, so be careful about depending on them.  If no de-duplication is 
   * desired (ie the Future is used as a key in a {@code Map}), then manually construct a new 
   * {@link ImmediateResultListenableFuture}.
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
    } else if (result == Optional.empty()) {
      return (ListenableFuture<T>)ImmediateResultListenableFuture.EMPTY_OPTIONAL_RESULT;
    } else if (result == Boolean.TRUE) {
      return (ListenableFuture<T>)ImmediateResultListenableFuture.BOOLEAN_TRUE_RESULT;
    } else if (result == Boolean.FALSE) {
      return (ListenableFuture<T>)ImmediateResultListenableFuture.BOOLEAN_FALSE_RESULT;
    } else if (result == "") { // equality check is ideal since the JVM will de-duplicate literal strings
      return (ListenableFuture<T>)ImmediateResultListenableFuture.EMPTY_STRING_RESULT;
    /* The below seem to impact performance a surprising amount
    } else if (result == Collections.emptyList()) {
      return (ListenableFuture<T>)ImmediateResultListenableFuture.EMPTY_LIST_RESULT;
    } else if (result == Collections.emptyMap()) {
      return (ListenableFuture<T>)ImmediateResultListenableFuture.EMPTY_MAP_RESULT;
    } else if (result == Collections.emptySet()) {
      return (ListenableFuture<T>)ImmediateResultListenableFuture.EMPTY_SET_RESULT;*/
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
                         firstRunAsync, RunnableCallableAdapter.adapt(task, null), 
                         (ignored) -> loopTest.get(), timeoutMillis, false);
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
                         (firstRunAsync ? scheduler : SameThreadSubmitterExecutor.instance()).submit(task), 
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
    
    ListenableFuture<T> alreadyDoneResult = shortcutAsyncWhile(startingFuture, loopTest);
    if (alreadyDoneResult != null) {
      return alreadyDoneResult;
    }
    
    long startTime = timeoutMillis > 0 ? Clock.accurateForwardProgressingMillis() : -1;
    // can not assert not resolved in parallel because user may cancel future at any point
    CancelDelegateSettableListenableFuture<T> resultFuture = 
        new CancelDelegateSettableListenableFuture<>(startingFuture, scheduler);
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
    
    startingFuture.callback(new FailurePropogatingFutureCallback<T>(resultFuture) {
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
            ListenableFuture<T> lf = 
                scheduler.submitScheduled(cancelCheckingTask, scheduleDelayMillis);
            resultFuture.updateDelegateFuture(lf);
            // TODO - if future is always already complete, this may StackOverflow
            lf.callback(this);  // add this to check again once execution completes
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
    }, null, null);
    return resultFuture;
  }
  
  /**
   * Similar to {@link #scheduleWhile(SubmitterScheduler, long, boolean, Callable, Predicate)} 
   * except that no executor is needed because the callable instead will return a future from the 
   * provided async submission (which may be a scheduled task or otherwise).
   * <p>
   * In the end this is just another way to have a loop of async actions, checking results as they 
   * are provided but not resolving the returned future till the async action completes.
   * 
   * @param <T> The result object type returned by the task and provided by the future
   * @param asyncTask Callable to produce a {@link ListenableFuture} for when a result is ready 
   * @param loopTest The test to check the ready result to see if we need to keep looping
   * @return Future that will resolve once returned {@link Predicate} returns {@code false}
   */
  public static <T> ListenableFuture<T> executeWhile(Callable<? extends ListenableFuture<? extends T>> asyncTask, 
                                                     Predicate<? super T> loopTest) {
    return executeWhile(asyncTask, loopTest, -1, false);
  }

  /**
   * Similar to {@link #scheduleWhile(SubmitterScheduler, long, boolean, Callable, Predicate, long, boolean)} 
   * except that no executor is needed because the callable instead will return a future from the 
   * provided async submission (which may be a scheduled task or otherwise).
   * <p>
   * In the end this is just another way to have a loop of async actions, checking results as they 
   * are provided but not resolving the returned future till the async action completes.  On a 
   * timeout, if {@code true} was provided for {@code timeoutProvideLastValue} then the future 
   * will be resolved with the last result provided.  If {@code false} was provided then the 
   * future will complete in an error state, the cause of which being a {@link TimeoutException}.
   * Canceling the returned future will prevent future executions from being attempted.  Canceling 
   * with an interrupt will transmit the interrupt to the running task if it is currently running.  
   * 
   * @param <T> The result object type returned by the task and provided by the future
   * @param asyncTask Callable to produce a {@link ListenableFuture} for when a result is ready 
   * @param loopTest The test to check the ready result to see if we need to keep looping
   * @param timeoutMillis If greater than zero, wont reschedule and instead will just return the last result
   * @param timeoutProvideLastValue On timeout {@code false} will complete with a TimeoutException, 
   *                                  {@code true} completes with the last result
   * @return Future that will resolve once returned {@link Predicate} returns {@code false}
   */
  public static <T> ListenableFuture<T> executeWhile(Callable<? extends ListenableFuture<? extends T>> asyncTask, 
                                                     Predicate<? super T> loopTest, 
                                                     long timeoutMillis, 
                                                     boolean timeoutProvideLastValue) {
    ArgumentVerifier.assertNotNull(asyncTask, "asyncTask");
    ArgumentVerifier.assertNotNull(loopTest, "loopTest");
    
    try {
      return executeWhile(asyncTask.call(), asyncTask, loopTest, 
                          timeoutMillis, timeoutProvideLastValue);
    } catch (Exception e) {
      return immediateFailureFuture(e);
    }
  }

  /**
   * Similar to {@link #scheduleWhile(SubmitterScheduler, long, ListenableFuture, Callable, Predicate)} 
   * except that no executor is needed because the callable instead will return a future from the 
   * provided async submission (which may be a scheduled task or otherwise).
   * <p>
   * In the end this is just another way to have a loop of async actions, checking results as they 
   * are provided but not resolving the returned future till the async action completes.
   * 
   * @param <T> The result object type returned by the task and provided by the future
   * @param startingFuture Future to use for first result to test for loop
   * @param asyncTask Callable to produce a {@link ListenableFuture} for when a result is ready 
   * @param loopTest The test to check the ready result to see if we need to keep looping
   * @return Future that will resolve once returned {@link Predicate} returns {@code false}
   */
  public static <T> ListenableFuture<T> executeWhile(ListenableFuture<? extends T> startingFuture, 
                                                     Callable<? extends ListenableFuture<? extends T>> asyncTask, 
                                                     Predicate<? super T> loopTest) {
    return executeWhile(startingFuture, asyncTask, loopTest, -1, false);
  }

  /**
   * Similar to {@link #scheduleWhile(SubmitterScheduler, long, ListenableFuture, Callable, Predicate, long, boolean)} 
   * except that no executor is needed because the callable instead will return a future from the 
   * provided async submission (which may be a scheduled task or otherwise).
   * <p>
   * In the end this is just another way to have a loop of async actions, checking results as they 
   * are provided but not resolving the returned future till the async action completes.  On a 
   * timeout, if {@code true} was provided for {@code timeoutProvideLastValue} then the future 
   * will be resolved with the last result provided.  If {@code false} was provided then the 
   * future will complete in an error state, the cause of which being a {@link TimeoutException}.
   * Canceling the returned future will prevent future executions from being attempted.  Canceling 
   * with an interrupt will transmit the interrupt to the running task if it is currently running.  
   * 
   * @param <T> The result object type returned by the task and provided by the future
   * @param startingFuture Future to use for first result to test for loop
   * @param asyncTask Callable to produce a {@link ListenableFuture} for when a result is ready 
   * @param loopTest The test to check the ready result to see if we need to keep looping
   * @param timeoutMillis If greater than zero, wont reschedule and instead will just return the last result
   * @param lastValueOnTimeout On timeout {@code false} will complete with a TimeoutException, 
   *                               {@code true} completes with the last result
   * @return Future that will resolve once returned {@link Predicate} returns {@code false}
   */
  public static <T> ListenableFuture<T> executeWhile(ListenableFuture<? extends T> startingFuture, 
                                                     Callable<? extends ListenableFuture<? extends T>> asyncTask, 
                                                     Predicate<? super T> loopTest, 
                                                     long timeoutMillis, boolean lastValueOnTimeout) {
    ArgumentVerifier.assertNotNull(startingFuture, "startingFuture");
    ArgumentVerifier.assertNotNull(asyncTask, "asyncTask");
    ArgumentVerifier.assertNotNull(loopTest, "loopTest");
    
    ListenableFuture<T> alreadyDoneResult = shortcutAsyncWhile(startingFuture, loopTest);
    if (alreadyDoneResult != null) {
      return alreadyDoneResult;
    }
    
    long startTime = timeoutMillis > 0 ? Clock.accurateForwardProgressingMillis() : -1;
    CancelDelegateSettableListenableFuture<T> resultFuture = 
        new CancelDelegateSettableListenableFuture<>(startingFuture, null);
    
    startingFuture.callback(new FailurePropogatingFutureCallback<T>(resultFuture) {
      @Override
      public void handleResult(T result) {
        resultFuture.setRunningThread(Thread.currentThread());
        try {
          while (loopTest.test(result)) {
            if (startTime > -1 && Clock.lastKnownForwardProgressingMillis() - startTime > timeoutMillis) {
              if (lastValueOnTimeout) {
                resultFuture.setResult(result);
              } else {
                resultFuture.setFailure(new TimeoutException());
              }
              return;
            }
            if (resultFuture.isCancelled()) {
              // already completed, just break the loop
              return;
            }
            ListenableFuture<? extends T> lf = asyncTask.call();
            if (lf.isDone()) {  // prevent StackOverflow when already done futures are returned
              try {
                result = lf.get();
                continue;
              } catch (ExecutionException e) {
                // uncaught exception already handled, don't handle twice
                resultFuture.setFailure(e.getCause());
                return;
              }
            } else {
              resultFuture.updateDelegateFuture(lf);
              lf.callback(this, null, null);
              return;
            }
          }
          // if broke loop without return, result is ready
          resultFuture.setResult(result);
        } catch (Throwable t) {
          // failure likely from the predicate test, handle exception 
          // so the behavior is closer to if the exception was thrown from a task submitted to the pool
          ExceptionUtils.handleException(t);
          
          resultFuture.setFailure(t);
        } finally {
          // unset running thread in case loop broke without final result
          resultFuture.setRunningThread(null);
        }
      }
    }, null, null);
    return resultFuture;
  }
  
  /**
   * Check if we can shortcut our async while loop by producing an already completed future.  If 
   * we can do this it will be returned, otherwise {@code null} will be returned.
   * 
   * @param startingFuture Future to check for already complete result
   * @param doneTest Test to see if the initial result is valid
   * @return A future that is already complete with the existing result, otherwise {@code null}
   */
  private static <T> ListenableFuture<T> shortcutAsyncWhile(ListenableFuture<? extends T> startingFuture, 
                                                            Predicate<? super T> doneTest) {
    if (startingFuture.isDone()) {
      if (startingFuture.isCancelled()) {
        return new ImmediateCanceledListenableFuture<>(null);
      }
      try {
        if (! doneTest.test(startingFuture.get())) {
          return immediateResultFuture(startingFuture.get());
        }
      } catch (ExecutionException e) {
        return immediateFailureFuture(e.getCause());
      } catch (Throwable t) {
        ExceptionUtils.handleException(t);
        return immediateFailureFuture(t);
      }
    }
    return null;
  }
}
