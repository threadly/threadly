package org.threadly.concurrent.future;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.threadly.concurrent.CallableContainer;
import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.event.RunnableListenerHelper;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.StackSuppressedRuntimeException;
import org.threadly.util.UnsafeAccess;

/**
 * This is a future which can be executed.  Allowing you to construct the future with the interior 
 * work, submit it to an {@link Executor}, and then return this future.
 * 
 * @since 1.0.0
 * @param <T> The result object type returned by this future
 */
public class ListenableFutureTask<T> extends FutureTask<T> 
                                     implements ListenableRunnableFuture<T>, 
                                                CallableContainer<T> {
  private static final Field RUNNING_THREAD_FIELD;
  private static final Field STATE_FIELD;
  private static final Field OUTCOME_FIELD;
  private static final Method AWAIT_DONE_METHOD;
  private static final int STATE_ERROR_VALUE = 3;
  private static final int STATE_CANCEL_THRESHOLD_VALUE = 4;
  
  static {
    Field runningThreadField = null;
    Field stateField = null;
    Field outcomeField = null;
    Method awaitDoneMethod = null;
    try {
      runningThreadField = FutureTask.class.getDeclaredField("runner");
      UnsafeAccess.setFieldToPublic(runningThreadField);
      stateField = FutureTask.class.getDeclaredField("state");
      UnsafeAccess.setFieldToPublic(stateField);
      outcomeField = FutureTask.class.getDeclaredField("outcome");
      UnsafeAccess.setFieldToPublic(outcomeField);
      awaitDoneMethod = FutureTask.class.getDeclaredMethod("awaitDone", boolean.class, long.class);
      UnsafeAccess.setMethodToPublic(awaitDoneMethod);
    } catch (NoSuchFieldException | NoSuchMethodException | SecurityException e) {
      ExceptionUtils.handleException(
          new RuntimeException("Unsupported JVM version, please update threadly or file an issue", e));
    } catch (RuntimeException e) {  // wrapped exception thrown from UnsafeAccess
      ExceptionUtils.handleException(e);
    } finally {
      RUNNING_THREAD_FIELD = runningThreadField;
      STATE_FIELD = stateField;
      OUTCOME_FIELD = outcomeField;
      AWAIT_DONE_METHOD = awaitDoneMethod;
    }
  }
  
  protected final RunnableListenerHelper listenerHelper;
  protected final boolean recurring;
  protected Callable<T> callable;
  private Executor executingExecutor;
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task runnable to be run
   */
  public ListenableFutureTask(boolean recurring, Runnable task) {
    this(recurring, task, null, null);
  }
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task runnable to be run
   * @param result result to be provide after run has completed
   */
  public ListenableFutureTask(boolean recurring, Runnable task, T result) {
    this(recurring, task, result, null);
  }

  /**
   * Constructs a runnable future with a callable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task callable to be run
   */
  public ListenableFutureTask(boolean recurring, Callable<T> task) {
    this(recurring, task, null);
  }
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task runnable to be run
   * @param result result to be provide after run has completed
   * @param executingExecutor Executor task will be run on for possible listener optimization, or {@code null}
   */
  public ListenableFutureTask(boolean recurring, Runnable task, T result, Executor executingExecutor) {
    this(recurring, RunnableCallableAdapter.adapt(task, result), executingExecutor);
  }

  /**
   * Constructs a runnable future with a callable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task callable to be run
   * @param executingExecutor Executor task will be run on for possible listener optimization, or {@code null}
   */
  public ListenableFutureTask(boolean recurring, Callable<T> task, Executor executingExecutor) {
    super(task);

    this.listenerHelper = new RunnableListenerHelper(true);
    this.recurring = recurring;
    this.callable = task;
    this.executingExecutor = executingExecutor;
  }
  
  @Override
  public void run() {
    if (recurring) {
      super.runAndReset();
    } else {
      super.run();
    }
  }

  @Override
  public ListenableFuture<T> listener(Runnable listener, Executor executor, 
                                      ListenerOptimizationStrategy optimize) {
    listenerHelper.addListener(listener, 
                               executor == executingExecutor && 
                                   (optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone | 
                                    optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatch) ? 
                                 null : executor, 
                               optimize == ListenerOptimizationStrategy.InvokingThreadIfDone | 
                               optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone ? 
                                 null : executor);
    return this;
  }
  
  /**
   * Can not be overridden, please use {@link #listener(Runnable)} as an alternative.
   */
  @Override
  protected final void done() {
    executingExecutor = null;
    callable = null;
    
    listenerHelper.callListeners();
  }

  @Override
  public Callable<T> getContainedCallable() {
    return callable;
  }
  
  @Override
  public StackTraceElement[] getRunningStackTrace() {
    try {
      Thread t = (Thread)RUNNING_THREAD_FIELD.get(this);
      if (t == null) {
        return null;
      } else {
        StackTraceElement[] stack = t.getStackTrace();
        if (stack.length == 0 || t != (Thread)RUNNING_THREAD_FIELD.get(this)) {
          return null;
        } else {
          return stack;
        }
      }
    } catch (RuntimeException | IllegalAccessException  e) {
      ExceptionUtils.handleException(
          new StackSuppressedRuntimeException("Stack access not supported, returning null" + 
                                                "...Please see first exception for more details", e));
      return null;
    }
  }
  

  @Override
  public boolean isCompletedExceptionally() {
    try {
      int state = STATE_FIELD.getInt(this);
      return state >= STATE_ERROR_VALUE;
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException("Unable to read JVM FutureTask state, please upgrade threadly", e);
    }
  }
  
  private int awaitDoneState(long awaitNanos) throws IllegalAccessException, IllegalArgumentException, InterruptedException {
    try {
      return ((Integer)AWAIT_DONE_METHOD.invoke(this, awaitNanos > 0, awaitNanos)).intValue();
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof InterruptedException) {
        throw (InterruptedException)cause;
      } else {
        throw new RuntimeException(cause);
      }
    }
  }

  @Override
  public Throwable getFailure() throws InterruptedException {
    try {
      int state = awaitDoneState(0L);
      if (state == STATE_ERROR_VALUE) {
        return (Throwable)OUTCOME_FIELD.get(this);
      } else if (state >= STATE_CANCEL_THRESHOLD_VALUE) {
        return new CancellationException();
      } else {
        return null;
      }
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException("Unable to read JVM FutureTask outcome", e);
    }
  }

  @Override
  public Throwable getFailure(long timeout, TimeUnit unit) throws InterruptedException,
                                                                  TimeoutException {
    try {
      int state = awaitDoneState(unit.toNanos(timeout));
      if (state <= 1) {
        throw new TimeoutException();
      } else if (state == STATE_ERROR_VALUE) {
        return (Throwable)OUTCOME_FIELD.get(this);
      } else if (state >= STATE_CANCEL_THRESHOLD_VALUE) {
        return new CancellationException();
      } else {
        return null;
      }
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException("Unable to read JVM FutureTask outcome", e);
    }
  }
}