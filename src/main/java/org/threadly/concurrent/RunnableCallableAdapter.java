package org.threadly.concurrent;

import java.util.concurrent.Callable;

import org.threadly.util.ArgumentVerifier;

/**
 * <p>Converts a {@link Runnable} with a result into a {@link Callable}.  This is similar to 
 * {@link java.util.concurrent.Executors#callable(Runnable, Object)}, except this implementation 
 * also implements the {@link RunnableContainer} interface.</p>
 * 
 * @author jent - Mike Jensen
 * @param <T> Type of result returned
 * @since 4.3.0
 */
public class RunnableCallableAdapter<T> implements Callable<T>, RunnableContainer {
  protected final Runnable runnable;
  protected final T result;
  
  /**
   * Constructs a new adapter with a provided runnable to execute, and an optional result.
   * 
   * @param runnable Runnable to be invoked when this adapter is ran
   * @param result Result to return from runnable or {@code null}
   */
  public RunnableCallableAdapter(Runnable runnable, T result) {
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
}
