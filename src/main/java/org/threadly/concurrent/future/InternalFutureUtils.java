package org.threadly.concurrent.future;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.future.ListenableFuture.ListenerOptimizationStrategy;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.SuppressedStackRuntimeException;

/**
 * Package protected utility classes and functions for operating on Futures.  Implementation 
 * heavily serves the functionality provided by {@link FutureUtils}.  This should not be depended 
 * on externally as it's API's should be considered unstable.
 * 
 * @since 5.29
 */
class InternalFutureUtils {
  
  protected static <T> ListenableFuture<T> immediateCanceledFuture() {
    SettableListenableFuture<T> slf = new SettableListenableFuture<>();
    slf.cancel(false);
    return slf;
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
   * @param reportedTransformedExceptions {@code true} to indicate transformer is not expected to throw exception.
   *                                          If any are thrown they will be delegated to 
   *                                          {@link ExceptionUtils#handleException(Throwable)}.
   * @param executor Executor to execute transformation function on, or {@code null}
   * @return Future with result of transformation function or respective error
   */
  protected static <ST, RT> ListenableFuture<RT> transform(ListenableFuture<ST> sourceFuture, 
                                                           Function<? super ST, ? extends RT> transformer, 
                                                           boolean reportedTransformedExceptions, 
                                                           Executor executor, 
                                                           ListenerOptimizationStrategy optimizeExecution) {
    if ((executor == null | optimizeExecution == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone) && 
        sourceFuture.isDone() && ! sourceFuture.isCancelled()) {
      try { // optimized path for already complete futures which we can now process in thread
        return FutureUtils.immediateResultFuture(transformer.apply(sourceFuture.get()));
      } catch (InterruptedException e) {  // should not be possible
        throw new RuntimeException(e);
      } catch (ExecutionException e) { // failure in getting result from future, transfer failure
        return FutureUtils.immediateFailureFuture(e.getCause());
      } catch (Throwable t) {
        if (reportedTransformedExceptions) {
          // failure calculating transformation, let handler get a chance to see the uncaught exception
          // This makes the behavior closer to if the exception was thrown from a task submitted to the pool
          ExceptionUtils.handleException(t);
        }
        
        return FutureUtils.immediateFailureFuture(t);
      }
    } else if (sourceFuture.isCancelled()) { // shortcut to avoid exception generation
      return immediateCanceledFuture();
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
            if (reportedTransformedExceptions) {
              // failure calculating transformation, let handler get a chance to see the uncaught exception
              // This makes the behavior closer to if the exception was thrown from a task submitted to the pool
              ExceptionUtils.handleException(t);
            }
            
            slf.setFailure(t);
          }
        }
      }, executor, optimizeExecution);
      return slf;
    }
  }
  
  /**
   * Similar to {@link #transform(ListenableFuture, Function, Executor, ListenerOptimizationStrategy)} 
   * except designed to handle functions which return futures.  This will take what otherwise 
   * would be {@code ListenableFuture<ListenableFuture<R>>}, and flattens it into a single future 
   * which will resolve once the contained future is complete.
   * 
   * @since 5.0
   * @param <ST> The source type for the object returned from the future and inputed into the mapper
   * @param <RT> The result type for the object contained in the future returned from the mapper
   * @param sourceFuture Future to source input into transformation function
   * @param transformer Function to apply result from future into returned future
   * @param executor Executor to execute transformation function on, or {@code null}
   * @return Future with result of transformation function or respective error
   */
  protected static <ST, RT> ListenableFuture<RT> flatTransform(ListenableFuture<? extends ST> sourceFuture, 
                                                               Function<? super ST, ListenableFuture<RT>> transformer, 
                                                               Executor executor, 
                                                               ListenerOptimizationStrategy optimizeExecution) {
    if ((executor == null | optimizeExecution == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone) && 
        sourceFuture.isDone() && ! sourceFuture.isCancelled()) {
      try { // optimized path for already complete futures which we can now process in thread
        return transformer.apply(sourceFuture.get());
      } catch (InterruptedException e) {  // should not be possible
        throw new RuntimeException(e);
      } catch (ExecutionException e) { // failure in getting result from future, transfer failure
        return FutureUtils.immediateFailureFuture(e.getCause());
      } catch (Throwable t) {
        // failure calculating transformation, let handler get a chance to see the uncaught exception
        // This makes the behavior closer to if the exception was thrown from a task submitted to the pool
        ExceptionUtils.handleException(t);
        
        return FutureUtils.immediateFailureFuture(t);
      }
    } else if (sourceFuture.isCancelled()) { // shortcut to avoid exception generation
      return immediateCanceledFuture();
    } else {
      CancelDelegateSettableListenableFuture<RT> slf = 
          new CancelDelegateSettableListenableFuture<>(sourceFuture, executor);
      sourceFuture.addCallback(new FailurePropogatingFutureCallback<ST>(slf) {
        @Override
        public void handleResult(ST result) {
          try {
            slf.setRunningThread(Thread.currentThread());
            ListenableFuture<? extends RT> mapFuture = transformer.apply(result);
            slf.updateCancelDelegateFuture(mapFuture);
            mapFuture.addCallback(slf);
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
   * Transform a future's failure condition into another future by applying the transformation 
   * function.  The mapper can then choose to either throw an exception, or convert the exception 
   * back into a normal result.
   * <p>
   * This can be easily used to chain together a series of operations, happening async until the 
   * final result is actually needed.  Once the future completes the mapper function will be invoked 
   * on the executor (if provided).  Because of that providing an executor can ensure this will 
   * never block.  If an executor is not provided then the mapper may be invoked on the calling 
   * thread (if the future is already complete), or on the same thread which the future completes 
   * on.
   * 
   * @since 5.17
   * @param <TT> The type of throwable that should be handled
   * @param <RT> The result type for the object returned from the mapper
   * @param sourceFuture Future to source input into transformation function
   * @param transformer Function to apply result from future into returned future
   * @param reportedTransformedExceptions {@code true} to indicate transformer is not expected to throw exception.
   *                                          If any are thrown they will be delegated to 
   *                                          {@link ExceptionUtils#handleException(Throwable)}.
   * @param executor Executor to execute transformation function on, or {@code null}
   * @return Future with result of transformation function or respective error
   */
  @SuppressWarnings("unchecked")
  protected static <TT extends Throwable, RT> ListenableFuture<RT> 
      failureTransform(ListenableFuture<RT> sourceFuture, Function<? super TT, ? extends RT> mapper,
                       Class<TT> throwableType, Executor executor, 
                       ListenerOptimizationStrategy optimizeExecution) {
    if ((executor == null | optimizeExecution == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone) && 
        sourceFuture.isDone()) { // optimized path for already complete futures which we can now process in thread
      if (sourceFuture.isCancelled()) { // shortcut to avoid exception generation
        if (throwableType == null || throwableType.isAssignableFrom(CancellationException.class)) {
          try {
            return FutureUtils.immediateResultFuture(mapper.apply((TT)new CancellationException()));
          } catch (Throwable t) {
            return FutureUtils.immediateFailureFuture(t);
          }
        } else {
          return sourceFuture;
        }
      } else {
        try {
          sourceFuture.get();
          return sourceFuture;  // no error
        } catch (InterruptedException e) {  // should not be possible
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          if (throwableType == null || throwableType.isAssignableFrom(e.getCause().getClass())) {
            try {
              return FutureUtils.immediateResultFuture(mapper.apply((TT)e.getCause()));
            } catch (Throwable t) {
              return FutureUtils.immediateFailureFuture(t);
            }
          } else {
            return sourceFuture;
          }
        }
      }
    }
    
    SettableListenableFuture<RT> slf = 
        new CancelDelegateSettableListenableFuture<>(sourceFuture, executor);
    // may still process in thread if future completed after check and executor is null
    sourceFuture.addCallback(new FutureCallback<RT>() {
      @Override
      public void handleResult(RT result) {
        slf.setResult(result);
      }
      
      @Override
      public void handleFailure(Throwable t) {
        if (throwableType == null || throwableType.isAssignableFrom(t.getClass())) {
          try {
            slf.setRunningThread(Thread.currentThread());
            slf.setResult(mapper.apply((TT)t));
          } catch (Throwable newT) {
            slf.setFailure(newT);
          }
        } else {
          slf.setFailure(t);
        }
      }
    }, executor, optimizeExecution);
    return slf;
  }

  /**
   * Similar to {@link #failureTransform(ListenableFuture, Function, Executor)} except designed to 
   * handle mapper functions which return futures.  This will take what otherwise would be 
   * {@code ListenableFuture<ListenableFuture<R>>}, and flattens it into a single future which will 
   * resolve once the contained future is complete.
   * 
   * @since 5.17
   * @param <TT> The type of throwable that should be handled
   * @param <RT> The result type for the object contained in the future returned from the mapper
   * @param sourceFuture Future to source input into transformation function
   * @param transformer Function to apply result from future into returned future
   * @param reportedTransformedExceptions {@code true} to indicate transformer is not expected to throw exception.
   *                                          If any are thrown they will be delegated to 
   *                                          {@link ExceptionUtils#handleException(Throwable)}.
   * @param executor Executor to execute transformation function on, or {@code null}
   * @return Future with result of transformation function or respective error
   */
  @SuppressWarnings("unchecked")
  protected static <TT extends Throwable, RT> ListenableFuture<RT> 
      flatFailureTransform(ListenableFuture<RT> sourceFuture, Function<? super TT, ListenableFuture<RT>> mapper,
                           Class<TT> throwableType, Executor executor, 
                           ListenerOptimizationStrategy optimizeExecution) {
    if ((executor == null | optimizeExecution == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone) && 
        sourceFuture.isDone()) { // optimized path for already complete futures which we can now process in thread
      if (sourceFuture.isCancelled()) { // shortcut to avoid exception generation
        if (throwableType == null || throwableType.isAssignableFrom(CancellationException.class)) {
          try {
            return mapper.apply((TT)new CancellationException());
          } catch (Throwable t) {
            return FutureUtils.immediateFailureFuture(t);
          }
        } else {
          return sourceFuture;
        }
      } else {
        try {
          sourceFuture.get();
          return sourceFuture;  // no error
        } catch (InterruptedException e) {  // should not be possible
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          if (throwableType == null || throwableType.isAssignableFrom(e.getCause().getClass())) {
            try {
              return mapper.apply((TT)e.getCause());
            } catch (Throwable t) {
              return FutureUtils.immediateFailureFuture(t);
            }
          } else {
            return sourceFuture;
          }
        }
      }
    }
    
    CancelDelegateSettableListenableFuture<RT> slf = 
        new CancelDelegateSettableListenableFuture<>(sourceFuture, executor);
    // may still process in thread if future completed after check and executor is null
    sourceFuture.addCallback(new FutureCallback<RT>() {
      @Override
      public void handleResult(RT result) {
        slf.setResult(result);
      }
      
      @Override
      public void handleFailure(Throwable t) {
        if (throwableType == null || throwableType.isAssignableFrom(t.getClass())) {
          try {
            slf.setRunningThread(Thread.currentThread());
            ListenableFuture<RT> mapFuture = mapper.apply((TT)t);
            slf.updateCancelDelegateFuture(mapFuture);
            mapFuture.addCallback(slf);
            slf.setRunningThread(null); // may be processing async now
          } catch (Throwable newT) {
            slf.setFailure(newT);
          }
        } else {
          slf.setFailure(t);
        }
      }
    }, executor, optimizeExecution);
    return slf;
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
      } else {
        settableFuture.handleFailure(t);
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
    private volatile ListenableFuture<?> cancelDelegateFuture;

    protected CancelDelegateSettableListenableFuture(ListenableFuture<?> lf, 
                                                     Executor executingExecutor) {
      super(false, executingExecutor);
      
      cancelDelegateFuture = lf;
    }
    
    public void updateCancelDelegateFuture(ListenableFuture<?> lf) {
      this.cancelDelegateFuture = lf;
    }
    
    @Override
    protected boolean setDone(Throwable cause) {
      if (super.setDone(cause)) {
        cancelDelegateFuture = null;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public StackTraceElement[] getRunningStackTrace() {
      ListenableFuture<?> delegateFuture = this.cancelDelegateFuture;
      if (delegateFuture != null) {
        StackTraceElement[] result = delegateFuture.getRunningStackTrace();
        if (result != null) {
          return result;
        }
      }
      return super.getRunningStackTrace();
    }
    
    protected boolean cancelRegardlessOfDelegateFutureState(boolean interruptThread) {
      ListenableFuture<?> cancelDelegateFuture = this.cancelDelegateFuture;
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
      }
      /**
       * The below code is inspired from the `super` implementation.  We must re-implement it in 
       * order to handle the case where canceling the delegate future may cancel ourselves (due to 
       * being a listener).  To solve this we synchronize the `resultLock` first, and know if we 
       * transition to `done` while holding the lock, it must be because we are a listener.
       * 
       * A simple nieve implementation may look like:
         if (cancelDelegateFuture.cancel(false)) {
           super.cancel(false);
           return true;
         } else {
           return false;
         }
       * This solves the listener problem by ignoring the need for `super` to cancel.  This likely 
       * will work for most situations, but has the risk that this future may have completed 
       * unexpectedly and we signal that it was canceled when really it completed with a result or 
       * failure.
       */
      if (isDone()) {
        return false;
      }
      
      boolean canceled = false;
      boolean callListeners = false;
      synchronized (resultLock) {
        if (! isDone()) {
          if (cancelDelegateFuture.cancel(false)) {
            canceled = true;
            if (! isDone()) { // may have transitioned to canceled already as a listener (within lock)
              callListeners = true;
              setCanceled();
            }
          }
        }
      }
      
      if (callListeners) {
        // call outside of lock
        listenerHelper.callListeners();
        runningThread = null;
      }
      
      return canceled;
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
      
      addListener(() -> futures = null);
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
      f.addListener(new FutureDoneTask(f), SameThreadSubmitterExecutor.instance());
    }
    
    @Override
    public boolean cancel(boolean interrupt) {
      // we need a copy in case canceling clears out the futures
      ArrayList<ListenableFuture<? extends T>> futures = this.futures;
      if (super.cancel(interrupt)) {
        FutureUtils.cancelIncompleteFutures(futures, interrupt);
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
      
      f.addListener(doneTaskSingleton, SameThreadSubmitterExecutor.instance());
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
        // should not be possible since this should only be called once the future is already done
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
        // should not be possible since this should only be called once the future is already done
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        super.handleFutureDone(f); // failed so add it
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
        FutureUtils.cancelIncompleteFutures(futures, interruptThread);
      }
    }
  }
}
