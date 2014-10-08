package org.threadly.test.concurrent;

import org.threadly.concurrent.NoThreadScheduler;
import org.threadly.util.Clock;

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
public class TestableScheduler extends NoThreadScheduler {
  private long nowInMillis;
  
  /**
   * Constructs a new {@link TestableScheduler} scheduler.
   */
  public TestableScheduler() {
    super(false);
    
    nowInMillis = Clock.lastKnownTimeMillis();
  }

  @Override
  protected long nowInMillis() {
    return nowInMillis;
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
    return tick(nowInMillis + timeInMillis);
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
    long currentRealTime = Clock.accurateTimeMillis();
    if (nowInMillis > currentRealTime) {
      return tick(nowInMillis);
    } else {
      return tick(currentRealTime);
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
    if (nowInMillis > currentTime) {
      throw new IllegalArgumentException("Time can not go backwards");
    }
    nowInMillis = currentTime;
    
    try {
      return super.tick();
    } catch (InterruptedException e) {
      // should not be possible with a false for blocking
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
