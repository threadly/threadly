package org.threadly.test.concurrent;

import java.util.List;

import org.threadly.concurrent.AbstractPriorityScheduler;
import org.threadly.concurrent.NoThreadScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionHandler;

/**
 * <p>This differs from {@link org.threadly.concurrent.NoThreadScheduler} in that time is ONLY 
 * advanced via the tick calls.  That means that if you schedule a task, it will be scheduled off 
 * of either the creation time, or the last tick time, what ever the most recent point is.  This 
 * allows you to progress time forward faster than it could in real time, having tasks execute 
 * faster, etc, etc.</p>
 * 
 * <p>The tasks in this scheduler are only progressed forward with calls to {@link #tick()}.  
 * Since it is running on the calling thread, calls to {@code Object.wait()} and 
 * {@code Thread.sleep()} from sub tasks will block (possibly forever).  The call to 
 * {@link #tick()} will not unblock till there is no more work for the scheduler to currently 
 * handle.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
public class TestableScheduler extends AbstractPriorityScheduler {
  private final InternalScheduler scheduler;
  private long nowInMillis;
  
  /**
   * Constructs a new {@link TestableScheduler} scheduler.
   */
  public TestableScheduler() {
    this(null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
  }
  
  /**
   * Constructs a new {@link TestableScheduler} scheduler.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   */
  public TestableScheduler(TaskPriority defaultPriority, long maxWaitForLowPriorityInMs) {
    super(defaultPriority);
    
    scheduler = new InternalScheduler(defaultPriority, maxWaitForLowPriorityInMs);
    nowInMillis = Clock.lastKnownTimeMillis();
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }

  @Override
  public int getActiveTaskCount() {
    return scheduler.getActiveTaskCount();
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay,
                                     TaskPriority priority) {
    scheduler.scheduleWithFixedDelay(task, initialDelay, recurringDelay, priority);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                  TaskPriority priority) {
    scheduler.scheduleAtFixedRate(task, initialDelay, period, priority);
  }

  @Override
  protected OneTimeTaskWrapper doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
    return scheduler.doSchedule(task, delayInMillis, priority);
  }

  @Override
  protected QueueManager getQueueManager() {
    return scheduler.getQueueManager();
  }
  
  /**
   * Returns the last provided time to the tick call.  If tick has not been called yet, then this 
   * will represent the time at construction.
   * 
   * @return last time the scheduler used for reference on execution
   */
  public long getLastTickTime() {
    return nowInMillis;
  }
  
  /**
   * This is to provide a convince when advancing the scheduler forward an explicit amount of time.  
   * Where tick accepts an absolute time, this accepts an amount of time to advance forward.  That 
   * way the user does not have to track the current time.
   * 
   * @param timeInMillis amount in milliseconds to advance the scheduler forward
   * @return quantity of tasks run during this tick call
   */
  public int advance(long timeInMillis) {
    return advance(timeInMillis, null);
  }
  
  /**
   * This is to provide a convince when advancing the scheduler forward an explicit amount of time.  
   * Where tick accepts an absolute time, this accepts an amount of time to advance forward.  That 
   * way the user does not have to track the current time.  
   * 
   * This call allows you to specify an {@link ExceptionHandler}.  If provided, if any tasks throw 
   * an exception, this will be called to inform them of the exception.  This allows you to ensure 
   * that you get a returned task count (meaning if provided, no exceptions except a possible 
   * {@link InterruptedException} can be thrown).  If {@code null} is provided for the exception 
   * handler, than any tasks which throw a {@link RuntimeException}, will throw out of this 
   * invocation.
   * 
   * @since 3.2.0
   * 
   * @param timeInMillis amount in milliseconds to advance the scheduler forward
   * @param exceptionHandler Exception handler implementation to call if any tasks throw an 
   *                           exception, or null to have exceptions thrown out of this call
   * @return quantity of tasks run during this tick call
   */
  public int advance(long timeInMillis, ExceptionHandler exceptionHandler) {
    return tick(nowInMillis + timeInMillis, exceptionHandler);
  }
  
  /**
   * Progresses tasks for the current time.  This will block as it runs as many scheduled or 
   * waiting tasks as possible.  This call will NOT block if no task are currently ready to run.
   * 
   * If any tasks throw a {@link RuntimeException}, they will be bubbled up to this tick call.  
   * Any tasks past that task will not run till the next call to tick.  So it is important that 
   * the implementor handle those exceptions.  
   * 
   * @return quantity of tasks run during this tick call
   */
  public int tick() {
    return tick(null);
  }
  
  /**
   * Progresses tasks for the current time.  This will block as it runs as many scheduled or 
   * waiting tasks as possible.  This call will NOT block if no task are currently ready to run.  
   * 
   * This call allows you to specify an {@link ExceptionHandler}.  If provided, if any tasks throw 
   * an exception, this will be called to inform them of the exception.  This allows you to ensure 
   * that you get a returned task count (meaning if provided, no exceptions except a possible 
   * {@link InterruptedException} can be thrown).  If {@code null} is provided for the exception 
   * handler, than any tasks which throw a {@link RuntimeException}, will throw out of this 
   * invocation.
   * 
   * @since 3.2.0
   * 
   * @param exceptionHandler Exception handler implementation to call if any tasks throw an 
   *                           exception, or null to have exceptions thrown out of this call
   * @return quantity of tasks run during this tick call
   */
  public int tick(ExceptionHandler exceptionHandler) {
    long currentRealTime = Clock.accurateTimeMillis();
    if (nowInMillis > currentRealTime) {
      return tick(nowInMillis, exceptionHandler);
    } else {
      return tick(currentRealTime, exceptionHandler);
    }
  }
  
  /**
   * This progresses tasks based off the time provided.  This is primarily used in testing by 
   * providing a possible time in the future (to execute future tasks).  This call will NOT block 
   * if no task are currently ready to run.  
   * 
   * If any tasks throw a {@link RuntimeException}, they will be bubbled up to this tick call.  
   * Any tasks past that task will not run till the next call to tick.  So it is important that 
   * the implementor handle those exceptions.
   * 
   * This call accepts the absolute time in milliseconds.  If you want to advance the scheduler a 
   * specific amount of time forward, look at the "advance" call.
   * 
   * @param currentTime Absolute time to provide for looking at task run time
   * @return quantity of tasks run in this tick call
   */
  public int tick(long currentTime) {
    return tick(currentTime, null);
  }
  
  /**
   * This progresses tasks based off the time provided.  This is primarily used in testing by 
   * providing a possible time in the future (to execute future tasks).  This call will NOT block 
   * if no task are currently ready to run.  
   * 
   * This call allows you to specify an {@link ExceptionHandler}.  If provided, if any tasks throw 
   * an exception, this will be called to inform them of the exception.  This allows you to ensure 
   * that you get a returned task count (meaning if provided, no exceptions except a possible 
   * {@link InterruptedException} can be thrown).  If {@code null} is provided for the exception 
   * handler, than any tasks which throw a {@link RuntimeException}, will throw out of this 
   * invocation.
   * 
   * This call accepts the absolute time in milliseconds.  If you want to advance the scheduler a 
   * specific amount of time forward, look at the "advance" call.
   * 
   * @since 3.2.0
   * 
   * @param currentTime Absolute time to provide for looking at task run time
   * @param exceptionHandler Exception handler implementation to call if any tasks throw an 
   *                           exception, or null to have exceptions thrown out of this call
   * @return quantity of tasks run in this tick call
   */
  public int tick(long currentTime, ExceptionHandler exceptionHandler) {
    if (nowInMillis > currentTime) {
      throw new IllegalArgumentException("Time can not go backwards");
    }
    nowInMillis = currentTime;
    
    return scheduler.tick(exceptionHandler);
  }
  
  /**
   * Checks if there are tasks ready to be run on the scheduler.  If 
   * {@link #tick(ExceptionHandler)} is not currently being called, this call indicates if the 
   * next {@link #tick(ExceptionHandler)} will have at least one task to run.  If 
   * {@link #tick(ExceptionHandler)} is currently being invoked, this call will do a best attempt 
   * to indicate if there is at least one more task to run (not including the task which may 
   * currently be running).  It's a best attempt as it will try not to block the thread invoking 
   * {@link #tick(ExceptionHandler)} to prevent it from accepting additional work.
   *  
   * @return {@code true} if there are task waiting to run
   */
  public boolean hasTaskReadyToRun() {
    return scheduler.hasTaskReadyToRun();
  }
  
  /**
   * Checks how long till the next task will be ready to execute.  If there are no tasks in this 
   * scheduler currently then {@link Long#MAX_VALUE} will be returned.  If there is a task ready 
   * to execute this will return a value less than or equal to zero.  If the task is past its 
   * desired point of execution the result will be a negative amount of milliseconds past that 
   * point in time.  
   * 
   * Generally this is called from the same thread that would invoke 
   * {@link #tick(ExceptionHandler)} (but does not have to be).  Since this does not block or lock 
   * if being invoked in parallel with {@link #tick(ExceptionHandler)}, the results may be no 
   * longer accurate by the time this invocation has returned.
   * 
   * This can be useful if you want to know how long you can block on something, ASSUMING you can 
   * detect that something has been added to the scheduler, and interrupt that blocking in order 
   * to handle tasks.
   * 
   * @return Milliseconds till the next task is ready to run
   */
  public long getDelayTillNextTask() {
    return scheduler.getDelayTillNextTask();
  }
  
  /**
   * Removes any tasks waiting to be run.  Will not interrupt any tasks currently running if 
   * {@link #tick(ExceptionHandler)} is being called.  But will avoid additional tasks from being 
   * run on the current {@link #tick(ExceptionHandler)} call.  
   * 
   * If tasks are added concurrently during this invocation they may or may not be removed.
   * 
   * @return List of runnables which were waiting in the task queue to be executed (and were now removed)
   */
  public List<Runnable> clearTasks() {
    return scheduler.clearTasks();
  }
  
  /**
   * <p>Small internal wrapper class so that we can control what from the "NoThreadScheduler" 
   * api's we want to expose from this implementation.</p>
   * 
   * @author jent - Mike Jensen
   * @since 2.4.0
   */
  private class InternalScheduler extends NoThreadScheduler {
    public InternalScheduler(TaskPriority defaultPriority, long maxWaitForLowPriorityInMs) {
      super(defaultPriority, maxWaitForLowPriorityInMs);
    }

    @Override
    protected long nowInMillis(boolean accurate) {
      return nowInMillis;
    }
    
    @Override
    protected QueueManager getQueueManager() {
      return super.getQueueManager();
    }
    
    @Override
    protected OneTimeTaskWrapper doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
      return super.doSchedule(task, delayInMillis, priority);
    }
  }
}
