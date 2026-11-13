package org.threadly.concurrent.future;

import static org.threadly.concurrent.future.InternalFutureUtils.invokeCompletedDirectly;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.threadly.concurrent.event.RunnableListenerHelper;
import org.threadly.util.Clock;

/**
 * This is an abstract implementation of a ListenableFuture which will be provided a result in the 
 * future.  Notable extending classes are {@link ListenableFutureTask} which provides a result from 
 * execution, as well as {@link SettableListenableFuture} which is provided a result from the 
 * program directly (or in mapping futures).  Because of the range of use this is a fairly 
 * complicated class, different implementations use different threading controls to make sure the 
 * result is set safely.  It is worth the complexity in order to share a single high performance 
 * implementation of thread parking for results as well as state management and interrupt controls.  
 * Prior to 7.0 there was some minor performance and behavior differences between 
 * {@link ListenableFutureTask} and {@link SettableListenableFuture}.
 * 
 * When a result is ready it should be provided to {@link #completeWithResult(Object)} or 
 * {@link #completeWithFailure(Throwable)}.
 * 
 * This class extends {@link AbstractCancellationMessageProvidingListenableFuture} in order to 
 * optionally provide cancel message control.  This can be ignored by simply not implementing the 
 * {@link AbstractCancellationMessageProvidingListenableFuture#getCancellationExceptionMessage()} 
 * function.
 * 
 * @since 7.0
 * @param <T> The result object type returned by this future
 */
abstract class AbstractCompletableListenableFuture<T> extends AbstractCancellationMessageProvidingListenableFuture<T> {
  /**
   * Initial state value when the future is constructed.
   */
  protected static final int STATE_NEW = 0;
  /**
   * State used to make sure a future can complete atomically.  The state transition from 
   * {@link STATE_NEW} can occur from multiple sources but we can not set a result until we know 
   * our thread is the one completing the future.  For that reason it is transitioned to this state
   * first.
   */
  protected static final int STATE_COMPLETING = 1;
  /**
   * Set when a future in {@link #STATE_COMPLETING} has been completed with a normal result through 
   * {@link #completeWithResult(Object)}.
   */
  protected static final int STATE_RESULT = 2;
  /**
   * Set when a future in {@link #STATE_COMPLETING} has been completed with a failure through 
   * {@link #completeWithFailure(Throwable)}.
   */
  protected static final int STATE_ERROR = 3;
  /**
   * Set when a future in {@link #STATE_NEW} has been canceled without an interrupt.
   */
  protected static final int STATE_CANCELLED = 4;
  /**
   * Set when a future in {@link #STATE_NEW} has been canceled with an interrupt.  In this 
   * condition the interrupt will be sent to the {@link #execThread}.
   */
  protected static final int STATE_INTERRUPTING = 5;
  /**
   * State value after the interrupt has been completed after {@link #STATE_INTERRUPTING}.
   */
  protected static final int STATE_INTERRUPTED = 6;
  /**
   * This is the offset applied to our state in the event of the result being cleared.  By adding 
   * this offset it allows us to read what the original / final state was.
   */
  protected static final int STATE_CLEARED_OFFSET = 100;
  
  private static final VarHandle STATE;
  private static final VarHandle PARKED_CHAIN;
  private static final VarHandle PARKED_THREAD_NEXT;
  
  static {
    try {
      MethodHandles.Lookup l = MethodHandles.lookup();
      STATE = l.findVarHandle(AbstractCompletableListenableFuture.class, "state", int.class);
      PARKED_CHAIN = l.findVarHandle(AbstractCompletableListenableFuture.class, "parkedChain", ParkedThread.class);
      PARKED_THREAD_NEXT = l.findVarHandle(ParkedThread.class, "next", ParkedThread.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
    
    // Match JDK FutureTask behavior: https://bugs.openjdk.java.net/browse/JDK-8074773
    @SuppressWarnings("unused")
    Class<?> ensureLoaded = LockSupport.class;
  }
  
  protected final RunnableListenerHelper listenerHelper;
  protected volatile Thread execThread;
  protected volatile int state;
  private volatile ParkedThread parkedChain;
  private volatile boolean listenerOptimized; // set if queued listener was optimized
  private Executor executingExecutor;
  // result and failure protected by state reads and writes
  private T result;
  private Throwable failure;
  
  protected AbstractCompletableListenableFuture(Executor executingExecutor) {
    this.listenerHelper = new RunnableListenerHelper(true);
    this.executingExecutor = executingExecutor;
  }
  
  /**
   * Invoked after the future has reached a completed state (with result, canceled, or in error).  
   * This will be invoked only once and by the thread completing the future execution.  It is 
   * designed to allow extending implementations to do any necessary cleanup after the result has 
   * been produced.
   */
  protected abstract void handleCompleteState();
  
  // MUST BE INVOKED BY COMPLETING THREAD, NOT CONCURRENTLY
  private void completeState() {
    ParkedThread pt;
    while ((pt = parkedChain) != null) {
      if (PARKED_CHAIN.weakCompareAndSet(this, pt, null)) {
        while (pt != null) {
          LockSupport.unpark(pt.thread);
          
          ParkedThread next = pt.next;
          pt.next = null; // allow GC
          pt = next;
        }
      }
    }

    handleCompleteState();
    executingExecutor = null;
  }

  /**
   * Complete the ListenableFuture with a provided failure.  If the provided failure is 
   * {@code null}, a new {@link Exception} will be created so that something is always provided in 
   * the {@link ExecutionException} on calls to {@link #get()}.
   *
   * @param t Throwable that caused failure during computation and should be reported by the future
   * @return {@code true} if the future was completed with the failure, {@code false} if already completed 
   */
  protected boolean completeWithFailure(Throwable t) {
    if (STATE.compareAndSet(this, STATE_NEW, STATE_COMPLETING)) {
      this.failure = t == null ? new Exception() : t;
      STATE.setRelease(this, STATE_ERROR);
      completeState();
      
      // callListeners invoked here instead of completeState() to reduce stack depth
      listenerHelper.callListeners();
      return true;
    } else {
      return false;
    }
  }

  /**
   * Complete the ListenableFuture with a provided result.
   * 
   * @param result result to provide for {@link #get()} call, can be {@code null}
   * @return {@code true} if the future was completed with the failure, {@code false} if already completed
   */
  protected boolean completeWithResult(T result) {
    if (STATE.compareAndSet(this, STATE_NEW, STATE_COMPLETING)) {
      this.result = result;
      STATE.setRelease(this, STATE_RESULT);
      completeState();
      
      // callListeners invoked here instead of completeState() to reduce stack depth
      listenerHelper.callListeners();
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * Clears the stored result from this set future.  This allows the result to be available for 
   * garbage collection.  After this call, future calls to {@link #get()} will throw an 
   * {@link IllegalStateException}.  So it is critical that this is only called after you are sure 
   * no future calls to get the result on this future will be attempted.
   * <p>
   * The design of this is so that if you want to chain {@link ListenableFuture}'s together, you 
   * can clear the results of old ones after their result has been consumed.  This is really only 
   * useful in very specific instances.
   * <p>
   * WARNING, this may produce some unexpected behaviors.  For example if the result is being pulled 
   * while this is concurrently invoked a {@code null} result may be returned when a result was 
   * previously available.  In addition {@link #get()} and {@link #callback(FutureCallback)} attempt 
   * to reliably error in this condition but {@link #failureCallback(Consumer)} and 
   * {@link #resultCallback(Consumer)} do not.  {@link #failureCallback(Consumer)} will only show 
   * the unexpected clearResult state if the future had finished in a failure condition we can't 
   * report.  And {@link #resultCallback(Consumer)} will always be a no-op after clearing.  In short
   * BE CAUTIOUS, only use this if you are certain the result or failure wont be re-queried and 
   * you understand the risks.
   */
  public void clearResult() {
    if (state == STATE_NEW) {
      throw new IllegalStateException("Result not set yet");
    }
    int state;
    try {
      // verify state is stable
      while ((state = awaitResultState(false, 0L)) == STATE_INTERRUPTING) {
        Thread.yield();
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException("Result not set yet", e);
    }
    
    STATE.setRelease(this, STATE_CLEARED_OFFSET + state);
    result = null;
    failure = null;
  }

  @Override
  public boolean cancel(boolean interrupt) {
    if (state != STATE_NEW || 
          ! STATE.compareAndSet(this, STATE_NEW, interrupt ? STATE_INTERRUPTING : STATE_CANCELLED)) {
      return false;
    }
    
    try {
      if (interrupt) {
        try {
          Thread t = execThread;
          if (t != null) {
            t.interrupt();
          }
        } finally {
          STATE.setRelease(this, STATE_INTERRUPTED);
        }
      }
    } finally {
      // Check reference before cleared in completeState
      Executor cancelExecutor = listenerOptimized ? executingExecutor : null;
      completeState();

      if (cancelExecutor == null) {
        listenerHelper.callListeners();
      } else {
        // Make sure even cancel actions happen on the expected pool
        // otherwise listeners with optimization may hit unexpected conditions
        // See https://github.com/threadly/threadly/issues/274
        cancelExecutor.execute(listenerHelper::callListeners);
      }
    }
    return true;
  }
  
  private Executor setListenerExecutorOptimizedAndReturnNull() {
    // The overhead of storing this state on every future is unfortunate.
    // The overhead of setting it every time a listener is added with a matching optimization 
    // strategy is also unfortunate.
    // But the alternative is to be conservative in cancel operations and make sure that listener 
    // execution always happens on the set executor.  Because this can delay listener executions 
    // it seems better to accept this tradeoff considering optimization usage is assumed to be rare. 
    listenerOptimized = true;
    return null;
  }

  @Override
  public ListenableFuture<T> listener(Runnable listener, Executor executor, 
                                      ListenerOptimizationStrategy optimize) {
    listenerHelper.addListener(listener, 
                               executor == executingExecutor && 
                                   (optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone | 
                                    optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatch) ? 
                                 setListenerExecutorOptimizedAndReturnNull() : executor, 
                               optimize == ListenerOptimizationStrategy.InvokingThreadIfDone | 
                               optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone ? 
                                 null : executor);
    return this;
  }
  
  @Override
  public StackTraceElement[] getRunningStackTrace() {
    Thread t = execThread;
    if (t == null) {
      return null;
    } else {
      StackTraceElement[] stack = t.getStackTrace();
      if (stack.length == 0 || t != execThread) {
        return null;
      } else {
        return stack;
      }
    }
  }

  @Override
  public boolean isCancelled() {
    int s = state;
    if (s > STATE_CLEARED_OFFSET) {
      return s - STATE_CLEARED_OFFSET >= STATE_CANCELLED;
    } else {
      return s >= STATE_CANCELLED;
    }
  }

  @Override
  public boolean isDone() {
    return state != STATE_NEW;
  }

  @Override
  public boolean isCompletedExceptionally() {
    int s = state;
    if (s > STATE_CLEARED_OFFSET) {
      return s - STATE_CLEARED_OFFSET >= STATE_ERROR;
    } else {
      return s >= STATE_ERROR;
    }
  }
  
  /**
   * Call to block until we reach a state which contains a result.  This means that {@link #result} 
   * or {@link #failure} are available (depending on the final state).  This does NOT necessarily 
   * mean that the state is stable or in a final value.  For example two notable conditions are the 
   * future may still be in a {@link #STATE_INTERRUPTING} state, or in the future may transition to 
   * a cleared state.
   * 
   * @param timed {@code true} if the {@code nanos} parameter should be respected
   * @param nanos Maximum nanoseconds to wait for a result state
   * @return The current state value after it has either reached a result state or the timeout was reached
   * @throws InterruptedException Thrown if the thread is interrupted while waiting for a result
   */
  protected int awaitResultState(boolean timed, long nanos) throws InterruptedException {
    if (timed && nanos <= 0) {
      return state;
    }
    long startTime = 0L;
    boolean queued = false;
    ParkedThread pt = null;
    try {
      while (true) {
        int s = state;
        if (s > STATE_COMPLETING) {
          return s;
        } else if (s == STATE_COMPLETING) {
          // Result should come shortly, yield loop till next state change
          Thread.yield();
        } else if (Thread.interrupted()) {
          throw new InterruptedException();
        } else if (! queued) {
          if (pt == null) {
            if (timed) {
              // initialize start time before possible slow or conflict operations
              startTime = Clock.accurateTimeNanos();
            }
            pt = new ParkedThread();
          }
          pt.next = parkedChain;
          queued = PARKED_CHAIN.weakCompareAndSet(this, pt.next, pt);
          // loop and recheck
        } else if (timed) {
          long elapsed = Clock.accurateTimeNanos() - startTime;
          if (elapsed >= nanos) {
            return state;
          }
          if (state < STATE_COMPLETING) {
            LockSupport.parkNanos(this, nanos - elapsed);
          }
        } else {
          LockSupport.park(this);
        }
      }
    } finally {
      if (queued) {
        // remove to keep queue tidy, but several things to consider
        // * DONT null out `next` of `pt` if set, this is to allow other threads traversing 
        //     the chain to not get lost / broken
        // * We use compareAndSet instead of weakCompareAndSet to do a best effort to avoid 
        //     re-traversing the parked chain to find our node in question
        retry: while (true) {
          ParkedThread prev = null;
          ParkedThread curr = parkedChain;
          while (curr != null) {
            if (curr == pt) {
              if (prev == null) {
                if (PARKED_CHAIN.compareAndSet(this, pt, pt.next)) {
                  break retry;
                } else {
                  continue retry;
                }
              } else {
                if (PARKED_THREAD_NEXT.compareAndSet(prev, pt, pt.next)) {
                  break retry;
                } else {
                  continue retry;
                }
              }
            } else {
              prev = curr;
              curr = curr.next;
            }
          }
          // assert isDone()
          // we did not find our entry, we know new queue entries had to be added in front
          break;
        }
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T get() throws InterruptedException, ExecutionException {
    int state = awaitResultState(false, 0L);
    if (state == STATE_ERROR) {
      throw new ExecutionException(failure);
    } else if (state > STATE_CLEARED_OFFSET) {
      throw new IllegalStateException("Result cleared, future get's not possible");
    } else if (state >= STATE_CANCELLED) {
      throw new CancellationException(getCancellationExceptionMessage());
    } else {
      return result;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                                                   TimeoutException {
    int state = awaitResultState(true, unit.toNanos(timeout));
    if (state < STATE_RESULT) {
      throw new TimeoutException();
    } else if (state == STATE_ERROR) {
      throw new ExecutionException(failure);
    } else if (state > STATE_CLEARED_OFFSET) {
      throw new IllegalStateException("Result cleared, future get's not possible");
    } else if (state >= STATE_CANCELLED) {
      throw new CancellationException(getCancellationExceptionMessage());
    } else {
      return result;
    }
  }

  @Override
  public Throwable getFailure() throws InterruptedException {
    int state = awaitResultState(false, 0L);
    if (state == STATE_ERROR) {
      return failure;
    } else if (state > STATE_CLEARED_OFFSET) {
      return new IllegalStateException("Result cleared, future get's not possible");
    } else if (state >= STATE_CANCELLED) {
      return new CancellationException(getCancellationExceptionMessage());
    } else {
      return null;
    }
  }

  @Override
  public Throwable getFailure(long timeout, TimeUnit unit) throws InterruptedException,
                                                                  TimeoutException {
    int state = awaitResultState(true, unit.toNanos(timeout));
    if (state < STATE_RESULT) {
      throw new TimeoutException();
    } else if (state == STATE_ERROR) {
      return failure;
    } else if (state > STATE_CLEARED_OFFSET) {
      return new IllegalStateException("Result cleared, future get's not possible");
    } else if (state >= STATE_CANCELLED) {
      return new CancellationException(getCancellationExceptionMessage());
    } else {
      return null;
    }
  }
  
  @Override
  public ListenableFuture<T> callback(FutureCallback<? super T> callback, Executor executor, 
                                      ListenerOptimizationStrategy optimize) {
    if (invokeCompletedDirectly(executor, optimize)) {
      int s = this.state;
      if (s == STATE_RESULT) {
        callback.handleResult(result);
        return this;
      } else if (s == STATE_ERROR) {
        callback.handleFailure(failure);
        return this;
      } else if (s > STATE_CLEARED_OFFSET) {
        callback.handleFailure(new IllegalStateException("Result cleared"));
        return this;
      } else if (s >= STATE_CANCELLED) {
        callback.handleFailure(new CancellationException(getCancellationExceptionMessage()));
        return this;
      }
    }
    
    listenerHelper.addListener(() -> {
                                 int s = this.state;
                                 if (s == STATE_RESULT) {
                                   callback.handleResult(result);
                                 } else if (s == STATE_ERROR) {
                                   callback.handleFailure(failure);
                                 } else if (s > STATE_CLEARED_OFFSET) {
                                   callback.handleFailure(new IllegalStateException("Result cleared"));
                                 } else if (s >= STATE_CANCELLED) {
                                   callback.handleFailure(new CancellationException(getCancellationExceptionMessage()));
                                 }
                               }, 
                               executor == executingExecutor && 
                                   (optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone | 
                                    optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatch) ? 
                                 setListenerExecutorOptimizedAndReturnNull() : executor, 
                               optimize == ListenerOptimizationStrategy.InvokingThreadIfDone | 
                               optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone ? 
                                 null : executor);
    return this;
  }
  
  @Override
  public ListenableFuture<T> resultCallback(Consumer<? super T> callback, Executor executor, 
                                            ListenerOptimizationStrategy optimize) {
    int s = state;
    if (s > STATE_RESULT) {
      return this;
    } else if (invokeCompletedDirectly(executor, optimize)) {
      if (s == STATE_RESULT) {
        callback.accept(result);
        return this;
      }
    }
    
    listenerHelper.addListener(() -> {
                                 if (state == STATE_RESULT) {
                                   callback.accept(result);
                                 }
                               }, 
                               executor == executingExecutor && 
                                   (optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone | 
                                    optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatch) ? 
                                 setListenerExecutorOptimizedAndReturnNull() : executor, 
                               optimize == ListenerOptimizationStrategy.InvokingThreadIfDone | 
                               optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone ? 
                                 null : executor);
    return this;
  }
  
  @Override
  public ListenableFuture<T> failureCallback(Consumer<Throwable> callback, Executor executor, 
                                             ListenerOptimizationStrategy optimize) {
    {
      int s = this.state;
      if (s == STATE_RESULT || s == STATE_RESULT + STATE_CLEARED_OFFSET) {
        // just like resultCallback silently ignores the cleared state, we will ignore it if the 
        // state was in a normal result state here
        return this;
      } else if (invokeCompletedDirectly(executor, optimize)) {
        if (s == STATE_ERROR) {
          callback.accept(failure);
          return this;
        } else if (s > STATE_CLEARED_OFFSET) {
          callback.accept(new IllegalStateException("Result cleared"));
          return this;
        } else if (s >= STATE_CANCELLED) {
          callback.accept(new CancellationException(getCancellationExceptionMessage()));
          return this;
        }
      }
    }
    
    listenerHelper.addListener(() -> {
                                 int s = this.state;
                                 if (s == STATE_ERROR) {
                                   callback.accept(failure);
                                 } else if (s > STATE_CLEARED_OFFSET) {
                                   if (s > STATE_RESULT + STATE_CLEARED_OFFSET) {
                                     callback.accept(new IllegalStateException("Result cleared"));
                                   }
                                 } else if (s >= STATE_CANCELLED) {
                                   callback.accept(new CancellationException(getCancellationExceptionMessage()));
                                 }
                               }, 
                               executor == executingExecutor && 
                                   (optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone | 
                                    optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatch) ? 
                                 setListenerExecutorOptimizedAndReturnNull() : executor, 
                               optimize == ListenerOptimizationStrategy.InvokingThreadIfDone | 
                               optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone ? 
                                 null : executor);
    return this;
  }
  
  @Override
  public String toString() {
    return toString(null);
  }
  
  protected String toString(Object task) {
    StringBuilder sb = new StringBuilder(100 /* generally a good default */);
    sb.append(super.toString());
    int s = state;
    boolean cleared = s > STATE_CLEARED_OFFSET;
    if (cleared) {
      s -= STATE_CLEARED_OFFSET;
    }
    // Formatting designed to match FutureTask toString()
    // This is to be API compatible from when ListenableFutureTask extended FutureTask
    switch (s) {
      case STATE_RESULT:
        sb.append("[Completed normally]");
        break;
      case STATE_ERROR:
        sb.append("[Completed exceptionally: ").append(cleared ? "CLEARED" : failure).append("]");
        break;
      case STATE_CANCELLED:
      case STATE_INTERRUPTING:
      case STATE_INTERRUPTED:
        sb.append("[Cancelled]");
        break;
      default:
        if (task == null) {
          sb.append("[Not completed]");
        } else {
          sb.append("[Not completed, task = ").append(task).append("]");
        }
        break;
    }
    if (cleared) {
      sb.append("[RESULT CLEARED]");
    }
    return sb.toString();
  }
  
  /**
   * Simple class to store a thread waiting for a result.  Used as a linked list through the 
   * {@link ParkedThread#next} variable which is typically adjusted through the 
   * {@link AbstractCompletableListenableFuture#PARKED_THREAD_NEXT} {@code VarHandle}.
   * 
   * @since 7.0
   */
  private static class ParkedThread {
    protected final Thread thread = Thread.currentThread();
    protected volatile ParkedThread next;
  }
}