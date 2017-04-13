package org.threadly.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.util.ArgumentVerifier;

/**
 * Abstract implementation for more complicated recurring behavior.  Unlike submitting a task to 
 * {@link SubmitterScheduler#scheduleWithFixedDelay(Runnable, long, long)}, this can provide the 
 * ability to only have the task scheduled if there is work to do.  In addition it provides the 
 * ability to change the frequency of execution without needing to remove and re-add the task.
 * <p>
 * This task will only schedule or reschedule itself if it has been notified there is work to do.  
 * It is assumed that after execution all work is complete.  If there is additional work to 
 * perform, then the task should invoke {@link #signalToRun()} before it completes to ensure that 
 * it is rescheduled at the current set delay.  Because of that behavior there is no way to remove 
 * this task from the scheduler, instead you must just ensure that {@link #signalToRun()} is not 
 * invoked to prevent the task from rescheduling itself.
 * <p>
 * An additional advantage to using this over scheduling a recurring task is that you don't have 
 * to worry about removing the task before garbage collection occurs (ie no cleanup, just stop 
 * invoking {@link #signalToRun()}).
 * 
 * @since 4.9.0
 */
public abstract class ReschedulingOperation {
  private final SubmitterScheduler scheduler;
  // -1 = not scheduled, 0 = scheduled, 1 = running, 2 = updated while running
  private final AtomicInteger taskState;
  private final CheckRunner runner;
  private volatile long scheduleDelay;
  
  /**
   * Construct a new operation with the provided scheduler to schedule on to and the initial delay.
   * 
   * @param scheduler Scheduler to execute on.
   * @param scheduleDelay Delay in milliseconds to schedule operation out when has stuff to do
   */
  protected ReschedulingOperation(SubmitterScheduler scheduler, long scheduleDelay) {
    ArgumentVerifier.assertNotNull(scheduler, "scheduler");
    
    this.scheduler = scheduler;
    this.taskState = new AtomicInteger(-1);
    this.runner = new CheckRunner();
    setScheduleDelay(scheduleDelay);
  }

  /**
   * Check to see if this operation is either currently running, or scheduled to run.
   * 
   * @return {@code true} means this operation still has things to do
   */
  public boolean isActive() {
    return taskState.get() != -1;
  }
  
  /**
   * Adjusts the delay at which this task is scheduled or rescheduled.  This can be invoked during 
   * {@link #run()} to change how long till it executes next.
   * 
   * @param scheduleDelay Delay in milliseconds to schedule operation out on, can not be negative
   */
  public void setScheduleDelay(long scheduleDelay) {
    ArgumentVerifier.assertNotNegative(scheduleDelay, "scheduleDelay");
    
    this.scheduleDelay = scheduleDelay;
  }
  
  /**
   * Invoke to indicate that this operation has stuff to do.  If necessary the task will be 
   * scheduled for execution.  If the task is already running then it will ensure the task 
   * re-executes itself when done (at the set delay).  This re-execution can help ensure that any 
   * thread state changes can be witnessed on the next execution.
   */
  public void signalToRun() {
    while (true) {
      if (taskState.get() == -1) {
        if (taskState.compareAndSet(-1, 0)) {
          scheduler.schedule(runner, scheduleDelay);
          return;
        }
      } else if (taskState.get() == 1) {
        if (taskState.compareAndSet(1, 2)) {
          return;
        }
      } else {
        // either already scheduled, or already marked as more added
        return;
      }
    }
  }
  
  /**
   * Abstract function which must be implemented to handle actual operation.  It is expected that 
   * when this runs all outstanding work is handled.  If it can not be fully handled then invoke 
   * {@link #signalToRun()} before returning.
   * <p>
   * If this throws an exception it will not impact the state of future executions (ie if 
   * {@link #signalToRun()} was invoked, the task will be rescheduled despite a thrown exception).
   */
  protected abstract void run();
  
  /**
   * Class to in a thread safe way update the execution state, and reschedule the task on 
   * completion if necessary.
   * 
   * @since 4.9.0
   */
  protected class CheckRunner implements Runnable {
    @Override
    public void run() {
      taskState.set(1);

      try {
        ReschedulingOperation.this.run();
      } finally {
        while (true) {
          if (taskState.get() == 1) {
            if (taskState.compareAndSet(1, -1)) {
              // nothing to run, just return
              break;
            }
          } else if (taskState.get() == 2) { // will be set back to 1 when this restarts
            scheduler.schedule(this, scheduleDelay);
            break;
          }
        }
      }
    }
    
    @Override
    public String toString() {
      return "CheckRunner for: " + ReschedulingOperation.this.toString();
    }
  }
}
