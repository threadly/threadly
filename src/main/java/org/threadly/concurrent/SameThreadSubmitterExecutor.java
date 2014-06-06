package org.threadly.concurrent;

import java.util.concurrent.Callable;

import org.threadly.concurrent.future.FutureUtils;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.util.ExceptionUtils;

/**
 * <p>A {@link SubmitterExecutorInterface} that will run all provided tasks 
 * immediately in the same thread that is calling into it.  This is different 
 * from calling the runnable directly only in that no exceptions will propagate 
 * out.  In the case of just "execute" the default UncaughtExceptionHandler 
 * will be provided the failure.  Otherwise thrown exceptions will be 
 * represented by their returned {@link ListenableFuture}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.2.0
 */
public class SameThreadSubmitterExecutor implements SubmitterExecutorInterface {
  private static final SameThreadSubmitterExecutor DEFAULT_INSTANCE;
  
  static {
    DEFAULT_INSTANCE = new SameThreadSubmitterExecutor();
  }
  
  /**
   * Call to get a default instance of the SameThreadSubmitterExecutor.  Because 
   * there is no saved or shared state, the same instance can be reused as much 
   * as desired.
   * 
   * @return an instance of SameThreadSubmitterExecutor
   */
  public static SameThreadSubmitterExecutor instance() {
    return DEFAULT_INSTANCE;
  }
  
  @Override
  public void execute(Runnable command) {
    if (command == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    try {
      command.run();
    } catch (Throwable t) {
      ExceptionUtils.handleException(t);
    }
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
    
    try {
      task.run();
      
      return FutureUtils.immediateResultFuture(result);
    } catch (RuntimeException e) {
      return FutureUtils.immediateFailureFuture(e);
    }
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    try {
      T result = task.call();
      
      return FutureUtils.immediateResultFuture(result);
    } catch (Exception e) {
      return FutureUtils.immediateFailureFuture(e);
    }
  }
}
