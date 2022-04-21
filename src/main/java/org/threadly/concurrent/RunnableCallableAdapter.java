package org.threadly.concurrent;

import java.util.concurrent.Callable;

import org.threadly.util.ArgumentVerifier;

/**
 * Converts a {@link Runnable} with a result into a {@link Callable}.  This is similar to 
 * {@link java.util.concurrent.Executors#callable(Runnable, Object)}, except this implementation 
 * also implements the {@link RunnableContainer} interface.
 * 
 * @since 4.3.0
 * @param <T> Type of result returned
 */
public class RunnableCallableAdapter<T> implements Callable<T>, RunnableContainer {
  /**
   * Adapt a {@link Runnable} and result into a {@link Callable}.  The returned callable will 
   * invoke {@link Runnable#run()} then return the result provided to this function.
   * 
   * @param <T> Type of result to be returned from provided {@link Callable}
   * @param runnable Runnable to be invoked when this adapter is ran
   * @param result Result to return from Callable or {@code null}
   * @return A {@link Callable} instance for invocation
   */
  @SuppressWarnings("unchecked")
  public static <T> Callable<T> adapt(Runnable runnable, T result) {
    if (runnable == DoNothingRunnable.instance()) {
      if (result == null) {
        return (Callable<T>) DoNothingCallable.INSTANCE;
      } else {
        return () -> result;
      }
    } else {
      return new RunnableCallableAdapter<>(runnable, result);
    }
  }
  
  protected final Runnable runnable;
  protected final T result;
  
  /**
   * Constructs a new adapter with a provided runnable to execute, and an optional result.
   * 
   * @param runnable Runnable to be invoked when this adapter is ran
   * @param result Result to return from Callable or {@code null}
   */
  protected RunnableCallableAdapter(Runnable runnable, T result) {
    ArgumentVerifier.assertNotNull(runnable, "runnable");
    
    this.runnable = runnable;
    this.result = result;
  }

  @Override
  public Runnable getContainedRunnable() {
    return runnable;
  }

  @Override
  public T call() {
    runnable.run();
    return result;
  }
  
  /**
   * Callable implementation which does no action, and just returns {@code null}.
   * 
   * @since 5.26
   */
  protected static final class DoNothingCallable implements Callable<Object> {
    protected static final DoNothingCallable INSTANCE = new DoNothingCallable();
    
    private DoNothingCallable() {
      // don't allow construction
    }
    
    @Override
    public final Object call() {
      return null;
    }
  }
}
