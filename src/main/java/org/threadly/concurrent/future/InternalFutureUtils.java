package org.threadly.concurrent.future;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
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
            slf.updateDelegateFuture(mapFuture);
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
            slf.updateDelegateFuture(mapFuture);
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
    private volatile ListenableFuture<?> delegateFuture;

    protected CancelDelegateSettableListenableFuture(ListenableFuture<?> lf, 
                                                     Executor executingExecutor) {
      super(false, executingExecutor);
      
      delegateFuture = lf;
    }
    
    public void updateDelegateFuture(ListenableFuture<?> lf) {
      this.delegateFuture = lf;
    }
    
    @Override
    protected boolean setDone(Throwable cause) {
      if (super.setDone(cause)) {
        delegateFuture = null;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public StackTraceElement[] getRunningStackTrace() {
      ListenableFuture<?> delegateFuture = this.delegateFuture;
      if (delegateFuture != null) {
        StackTraceElement[] result = delegateFuture.getRunningStackTrace();
        if (result != null) {
          return result;
        }
      }
      return super.getRunningStackTrace();
    }
    
    protected boolean cancelRegardlessOfDelegateFutureState(boolean interruptThread) {
      ListenableFuture<?> cancelDelegateFuture = this.delegateFuture;
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
          if (delegateFuture.cancel(false)) {
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
    @SuppressWarnings("rawtypes")
    private static final ListenableFuture[] EMPTY_ARRAY = new ListenableFuture[0];
    
    protected final AtomicInteger remainingResult;
    private ArrayList<ListenableFuture<? extends T>> futures;
    protected ListenableFuture<? extends T>[] buildingResult;
    
    
    @SuppressWarnings("unchecked")
    protected FutureCollection(Iterable<? extends ListenableFuture<? extends T>> source) {
      super(false);
      
      remainingResult = new AtomicInteger(0); // may go negative if results finish before all are added
      futures = new ArrayList<>();
      buildingResult = EMPTY_ARRAY;

      int count = 0;
      if (source != null) {
        Iterator<? extends ListenableFuture<? extends T>> it = source.iterator();
        while (it.hasNext()) {
          ListenableFuture<? extends T> f = it.next();
          futures.add(f);
          attachFutureDoneTask(f, count++);
        }
      }
      
      init(count);
      
      // we need to verify that all futures have not already completed
      if (remainingResult.addAndGet(count) == 0) {
        setResult(getFinalResultList());
      } else {
        futures.trimToSize();
      }
      
      addListener(() -> futures = null);
    }
    
    /**
     * Called to inform expected future sizes.  {@link #attachFutureDoneTask(ListenableFuture, int)} 
     * will be invoked BEFORE this.  In addition those futures may complete before init is invoked, 
     * so this is more of a hint for future optimization.
     * 
     * @param futureCount Total number of futures in this collection
     */
    protected void init(int futureCount) {
      synchronized (this) {
        ensureCapacity(futureCount);
      }
    }
    
    // MUST synchronize `this` before calling
    @SuppressWarnings("unchecked")
    protected ListenableFuture<? extends T>[] ensureCapacity(int capacity) {
      if (buildingResult.length < capacity) {
        buildingResult = Arrays.copyOf(buildingResult, capacity);
      }
      return buildingResult;
    }
    
    /**
     * Adds item to the result list.  This list may be lazily constructed and thus why you must add 
     * through this function rather than directly on to the list.
     */
    protected void addResult(ListenableFuture<? extends T> f, int index) {
      synchronized (this) {
        ensureCapacity(index + 1)[index] = f;
      }
    }
    
    /**
     * Attach a {@link FutureDoneTask} to the provided future.  This is necessary for tracking as 
     * futures complete, failing to attach a task could result in this future never completing.  
     * <p>
     * This is provided as a separate function so it can be overriden to provide different 
     * {@link FutureDoneTask} implementation.
     * 
     * @param f Future to attach to
     * @param index The index associated to the future
     */
    protected void attachFutureDoneTask(ListenableFuture<? extends T> f, int index) {
      f.addListener(new FutureDoneTask(f, index), SameThreadSubmitterExecutor.instance());
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
     * Gives the implementing class the option to save or check the completed future.
     * 
     * @param f {@link ListenableFuture} that has completed
     * @param index The index associated to the future
     */
    protected abstract void handleFutureDone(ListenableFuture<? extends T> f, int index);

    /**
     * Will only be called once, and all allocated resources can be freed after this point.
     * 
     * @return List to satisfy ListenableFuture result with
     */
    protected List<ListenableFuture<? extends T>> getFinalResultList() {
      if (buildingResult == EMPTY_ARRAY) {
        return Collections.emptyList();
      } else {
        List<ListenableFuture<? extends T>> result = Arrays.asList(buildingResult);
        buildingResult = null;
        return result;
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
      private final int index;
      
      protected FutureDoneTask(ListenableFuture<? extends T> f, int index) {
        this.f = f;
        this.index = index;
      }
      
      @Override
      public void run() {
        try {  // exceptions should not be possible, but done for robustness
          handleFutureDone(f, index);
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
    private Runnable doneTaskSingleton;
    
    protected EmptyFutureCollection(Iterable<? extends ListenableFuture<?>> source) {
      super(source);
    }
    
    @Override
    protected void init(int futureCount) {
      // don't init collection, result ignored
    }

    @Override
    protected void handleFutureDone(ListenableFuture<?> f, int index) {
      // should not be invoked due to override attachFutureDoneTask
      throw new UnsupportedOperationException();
    }
    
    @Override
    protected List<ListenableFuture<?>> getFinalResultList() {
      return null;
    }
    
    @Override
    protected void attachFutureDoneTask(ListenableFuture<?> f, int index) {
      // we don't care about the result of the future
      // so to save a little memory we reuse the same task with no future provided
      // must be lazily set due to being invoked from super constructor
      if (doneTaskSingleton == null) {
        doneTaskSingleton = () -> {
          if (remainingResult.decrementAndGet() == 0) {
            setResult(getFinalResultList());
          }
        };
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
    protected void handleFutureDone(ListenableFuture<? extends T> f, int index) {
      addResult(f, index);
    }
  }
  
  /**
   * A future implementation that will return a List of futures as the result.  The future will 
   * not be satisfied till all provided futures have completed.
   * 
   * @since 5.30
   * @param <T> The result object type returned from the futures
   */
  protected static abstract class PartialFutureCollection<T> extends FutureCollection<T> {
    protected PartialFutureCollection(Iterable<? extends ListenableFuture<? extends T>> source) {
      super(source);
    }
    
    @Override
    protected List<ListenableFuture<? extends T>> getFinalResultList() {
      List<ListenableFuture<? extends T>> superResult = super.getFinalResultList();
      
      for (int i = 0; i < superResult.size(); i++) {  // should be RandomAccess list
        if (superResult.get(i) == null) { // indicates we must manipulate list
          for (i = i == 0 ? 1 : 0; i < superResult.size(); i++) {
            if (superResult.get(i) != null) { // found first item that must be included
              ArrayList<ListenableFuture<? extends T>> result = 
                  new ArrayList<>(superResult.size() - Math.max(i, 1));
              for (; i < superResult.size(); i++) {
                ListenableFuture<? extends T> lf = superResult.get(i);
                if (lf != null) {
                  result.add(lf);
                }
              }
              return result;
            }
          }
          return Collections.emptyList(); // all items were null
        }
      }
      
      return superResult; // no null found
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
  protected static class SuccessFutureCollection<T> extends PartialFutureCollection<T> {
    protected SuccessFutureCollection(Iterable<? extends ListenableFuture<? extends T>> source) {
      super(source);
    }

    @Override
    protected void handleFutureDone(ListenableFuture<? extends T> f, int index) {
      if (f.isCancelled()) {
        // detect canceled conditions before an exception would have otherwise thrown
        // canceled futures are ignored
        return;
      }
      try {
        f.get();
        addResult(f, index);  // if no exception thrown, add future
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
  protected static class FailureFutureCollection<T> extends PartialFutureCollection<T> {
    protected FailureFutureCollection(Iterable<? extends ListenableFuture<? extends T>> source) {
      super(source);
    }
    
    @Override
    protected void init(int futureCount) {
      // don't init collection, optimize for failures being rare
    }

    @Override
    protected void handleFutureDone(ListenableFuture<? extends T> f, int index) {
      if (f.isCancelled()) {
        // detect canceled conditions before an exception would have otherwise thrown 
        addResult(f, index);
        return;
      }
      try {
        f.get();
      } catch (InterruptedException e) {
        // should not be possible since this should only be called once the future is already done
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        addResult(f, index); // failed so add it
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
