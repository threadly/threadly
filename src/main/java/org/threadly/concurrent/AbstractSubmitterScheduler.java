package org.threadly.concurrent;

import java.util.concurrent.Callable;

import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.util.ArgumentVerifier;

/**
 * <p>Similar to the {@link AbstractSubmitterExecutor} this abstract class is designed to reduce 
 * code duplication for the multiple schedule functions.  This includes error checking, as well as 
 * wrapping things up in {@link ListenableFutureTask}'s if necessary.  In general this wont be 
 * useful outside of Threadly developers, but must be a public interface since it is used in 
 * sub-packages.</p>
 * 
 * <p>If you do find yourself using this class, please post an issue on github to tell us why.  If 
 * there is something you want our schedulers to provide, we are happy to hear about it.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
@SuppressWarnings("deprecation")
public abstract class AbstractSubmitterScheduler extends AbstractSubmitterExecutor
                                                 implements SubmitterSchedulerInterface {
  @Override
  protected void doExecute(Runnable task) {
    doSchedule(task, 0);
  }

  /**
   * Should schedule the provided task.  All error checking has completed by this point.
   * 
   * @param task Runnable ready to be ran
   * @param delayInMillis delay to schedule task out to
   */
  protected abstract void doSchedule(Runnable task, long delayInMillis);
  
  @Override
  public void schedule(Runnable task, long delayInMs) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");
    
    doSchedule(task, delayInMs);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs) {
    return submitScheduled(task, null, delayInMs);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task, result);

    doSchedule(lft, delayInMs);
    
    return lft;
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task);

    doSchedule(lft, delayInMs);
    
    return lft;
  }
}
