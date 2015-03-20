package org.threadly.test.concurrent;

import java.util.concurrent.Callable;

import org.threadly.concurrent.AbstractSubmitterScheduler;
import org.threadly.concurrent.NoThreadScheduler;
import org.threadly.concurrent.SchedulerServiceInterface;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionHandlerInterface;

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
public class TestableScheduler extends AbstractSubmitterScheduler 
                               implements SchedulerServiceInterface {
  private final InternalScheduler scheduler;
  private long nowInMillis;
  
  /**
   * Constructs a new {@link TestableScheduler} scheduler.
   */
  public TestableScheduler() {
    scheduler = new InternalScheduler();
    nowInMillis = Clock.lastKnownTimeMillis();
  }

  @Override
  public boolean remove(Runnable task) {
    return scheduler.remove(task);
  }

  @Override
  public boolean remove(Callable<?> task) {
    return scheduler.remove(task);
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }

  @Override
  protected void doSchedule(Runnable task, long delayInMillis) {
    scheduler.schedule(task, delayInMillis);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
    scheduler.scheduleWithFixedDelay(task, initialDelay, recurringDelay);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    scheduler.scheduleAtFixedRate(task, initialDelay, period);
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
   * This call allows you to specify an {@link ExceptionHandlerInterface}.  If provided, if any 
   * tasks throw an exception, this will be called to inform them of the exception.  This allows 
   * you to ensure that you get a returned task count (meaning if provided, no exceptions except 
   * a possible {@link InterruptedException} can be thrown).  If null is provided for the 
   * exception handler, than any tasks which throw a {@link RuntimeException}, will throw out of 
   * this invocation.
   * 
   * @since 3.2.0
   * 
   * @param timeInMillis amount in milliseconds to advance the scheduler forward
   * @param exceptionHandler Exception handler implementation to call if any tasks throw an 
   *                           exception, or null to have exceptions thrown out of this call
   * @return quantity of tasks run during this tick call
   */
  public int advance(long timeInMillis, ExceptionHandlerInterface exceptionHandler) {
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
   * This call allows you to specify an {@link ExceptionHandlerInterface}.  If provided, if any 
   * tasks throw an exception, this will be called to inform them of the exception.  This allows 
   * you to ensure that you get a returned task count (meaning if provided, no exceptions except 
   * a possible {@link InterruptedException} can be thrown).  If null is provided for the 
   * exception handler, than any tasks which throw a {@link RuntimeException}, will throw out of 
   * this invocation.
   * 
   * @since 3.2.0
   * 
   * @param exceptionHandler Exception handler implementation to call if any tasks throw an 
   *                           exception, or null to have exceptions thrown out of this call
   * @return quantity of tasks run during this tick call
   */
  public int tick(ExceptionHandlerInterface exceptionHandler) {
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
   * This call allows you to specify an {@link ExceptionHandlerInterface}.  If provided, if any 
   * tasks throw an exception, this will be called to inform them of the exception.  This allows 
   * you to ensure that you get a returned task count (meaning if provided, no exceptions except 
   * a possible {@link InterruptedException} can be thrown).  If null is provided for the 
   * exception handler, than any tasks which throw a {@link RuntimeException}, will throw out of 
   * this invocation.
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
  public int tick(long currentTime, ExceptionHandlerInterface exceptionHandler) {
    if (nowInMillis > currentTime) {
      throw new IllegalArgumentException("Time can not go backwards");
    }
    nowInMillis = currentTime;
    
    return scheduler.tick(exceptionHandler);
  }
  
  /**
   * <p>Small internal wrapper class so that we can control what from the "NoThreadScheduler" 
   * api's we want to expose from this implementation.</p>
   * 
   * @author jent - Mike Jensen
   * @since 2.4.0
   */
  private class InternalScheduler extends NoThreadScheduler {
    @Override
    protected long nowInMillis() {
      return nowInMillis;
    }
  }
}
