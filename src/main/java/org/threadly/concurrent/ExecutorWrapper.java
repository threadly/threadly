package org.threadly.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;

/**
 * <p>A simple wrapper class for {@link Executor} implementations to 
 * provide {@link SubmitterExecutorInterface} capabilities.</p>
 * 
 * <p>In addition this implementation returns {@link ListenableFuture} 
 * future implementations, making it an easy way to convert your favorite 
 * executor to use ListenableFutures.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class ExecutorWrapper implements SubmitterExecutorInterface {
  protected final Executor executor;
  
  /**
   * Constructors a new wrapper instance with the provided 
   * executor to defer calls to.
   * 
   * @param executor {@link Executor} instance.
   */
  public ExecutorWrapper(Executor executor) {
    if (executor == null) {
      throw new IllegalArgumentException("Must provide executor implementation");
    }
    
    this.executor = executor;
  }
  
  @Override
  public void execute(Runnable task) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    executor.execute(task);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task) {
    return submit(task, null);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task, result);
    
    executor.execute(lft);
    
    return lft;
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task);
    
    executor.execute(lft);
    
    return lft;
  }
}
