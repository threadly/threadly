package org.threadly.concurrent;

import java.util.concurrent.Callable;

import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.util.ArgumentVerifier;

/**
 * <p>Since the conversion to a {@link SubmitterExecutor} from an executor is often the same (just 
 * using the {@link ListenableFutureTask} to wrap the task).  This class provides an easy way to 
 * create a {@link SubmitterExecutor}.  Take a look at {@link ExecutorWrapper} for an easy example 
 * of how this is used.  In general this wont be useful outside of Threadly developers, but must 
 * be a public interface since it is used in sub-packages.</p>
 * 
 * <p>If you do find yourself using this class, please post an issue on github to tell us why.  If 
 * there is something you want our schedulers to provide, we are happy to hear about it.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.3.0
 */
@SuppressWarnings("deprecation")
public abstract class AbstractSubmitterExecutor implements SubmitterExecutorInterface {
  /**
   * Should execute the provided task, or provide the task to a given executor.  All error 
   * checking has completed by this point.
   * 
   * @param task Runnable ready to be ran
   */
  protected abstract void doExecute(Runnable task);
  
  @Override
  public void execute(Runnable task) {
    ArgumentVerifier.assertNotNull(task, "task");
    
    doExecute(task);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task) {
    return submit(task, null);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result) {
    ArgumentVerifier.assertNotNull(task, "task");
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task, result);
    
    doExecute(lft);
    
    return lft;
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    ArgumentVerifier.assertNotNull(task, "task");
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task);
    
    doExecute(lft);
    
    return lft;
  }
}
