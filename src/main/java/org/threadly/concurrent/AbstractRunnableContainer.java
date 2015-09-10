package org.threadly.concurrent;

/**
 * <p>Abstract runnable container.  This class just simply holds a runnable and implements 
 * {@link RunnableContainer}.  The main goal of wrapping a runnable would be so that 
 * {@link SchedulerService#remove(Runnable)} could detect the runnable contained inside this 
 * class.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.3.0
 */
public abstract class AbstractRunnableContainer implements RunnableContainer {
  protected final Runnable runnable;
  
  protected AbstractRunnableContainer(Runnable runnable) {
    this.runnable = runnable;
  }

  @Override
  public final Runnable getContainedRunnable() {
    return runnable;
  }
}
