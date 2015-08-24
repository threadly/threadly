package org.threadly.test.concurrent;

import java.util.concurrent.Callable;

import org.threadly.concurrent.PrioritySchedulerInterface;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.ListenableFuture;

/**
 * <p>This is similar to {@link TestableScheduler} except that it implements the 
 * {@link PrioritySchedulerInterface}.  This allows you to use a this testable implementation in 
 * code which requires a {@link PrioritySchedulerInterface}.</p>
 * 
 * <p>Since {@link TaskPriority} makes no sense for either a {@link TestableScheduler} due to the 
 * single threaded nature of it.  The priority in this implementation is simply ignored.  This is 
 * only provided to allow compatibility with the {@link PrioritySchedulerInterface}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 3.4.0
 */
@SuppressWarnings({ "deprecation", "javadoc" })
public class TestablePriorityScheduler extends TestableScheduler 
                                       implements PrioritySchedulerInterface {
  @Override
  public void execute(Runnable task, TaskPriority priority) {
    super.execute(task);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task, TaskPriority priority) {
    return super.submit(task);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result, TaskPriority priority) {
    return super.submit(task, result);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task, TaskPriority priority) {
    return super.submit(task);
  }

  @Override
  public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
    super.schedule(task, delayInMs);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs, 
                                             TaskPriority priority) {
    return super.submitScheduled(task, delayInMs);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs,
                                                 TaskPriority priority) {
    return super.submitScheduled(task, result, delayInMs);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs,
                                                 TaskPriority priority) {
    return super.submitScheduled(task, delayInMs);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay,
                                     TaskPriority priority) {
    super.scheduleWithFixedDelay(task, initialDelay, recurringDelay);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                  TaskPriority priority) {
    super.scheduleAtFixedRate(task, initialDelay, period);
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return TaskPriority.High;
  }
}
