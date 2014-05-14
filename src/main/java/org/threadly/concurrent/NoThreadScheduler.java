package org.threadly.concurrent;

import org.threadly.util.Clock;

/**
 * <p>Executor which has no threads itself.  This allows you to have the same 
 * scheduler abilities (schedule tasks, recurring tasks, etc, etc), without having 
 * to deal with multiple threads, memory barriers, or other similar concerns.  
 * This class can be very useful in GUI development (if you want it to run on the GUI 
 * thread).  It also can be useful in android development in a very similar way.</p>
 * 
 * <p>The tasks in this scheduler are only progressed forward with calls to .tick().  
 * Since it is running on the calling thread, calls to .wait() and .sleep() from sub 
 * tasks will block (possibly forever).  The call to .tick() will not unblock till there 
 * is no more work for the scheduler to currently handle.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
public class NoThreadScheduler extends AbstractTickableScheduler {
  /**
   * Constructs a new {@link NoThreadScheduler} scheduler.
   * 
   * @param tickBlocksTillAvailable true if calls to .tick() should block till there is something to run
   */
  public NoThreadScheduler(boolean tickBlocksTillAvailable) {
    super(tickBlocksTillAvailable);
  }

  @Override
  protected long nowInMillis() {
    return Clock.accurateTime();
  }
  
  /**
   * Progresses tasks for the current time.  This will block as it runs
   * as many scheduled or waiting tasks as possible.
   * 
   * Depending on how this class was constructed, this may or may not block 
   * if there are no tasks to run yet.
   * 
   * If any tasks throw a RuntimeException, they will be bubbled up to this 
   * tick call.  Any tasks past that task will not run till the next call to 
   * tick.  So it is important that the implementor handle those exceptions.  
   * 
   * @return qty of steps taken forward.  Returns zero if no events to run.
   * @throws InterruptedException thrown if thread is interrupted waiting for task to run
   *           (this can only throw if constructed with a true to allow blocking)
   */
  public int tick() throws InterruptedException {
    return super.tick();
  }
  
  /**
   * Checks if there are tasks ready to be run on the scheduler.  If this returns 
   * true, the next .tick() call is guaranteed to run at least one task.
   * 
   * @return true if there are task waiting to run.
   */
  public boolean hasTaskReadyToRun() {
    return getNextReadyTask() != null;
  }
  
  /**
   * Removes any tasks waiting to be run.  Will not interrupt any tasks currently running if 
   * .tick() is being called.  But will avoid additional tasks from being run on the current 
   * .tick() call.
   */
  public void clearTasks() {
    taskQueue.clear();
  }
}
