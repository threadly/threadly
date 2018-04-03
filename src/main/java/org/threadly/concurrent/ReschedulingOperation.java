package org.threadly.concurrent;

import java.util.concurrent.Executor;
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
  protected final Executor executor;  // never null
  private final SubmitterScheduler scheduler; // may be null
  // -1 = not scheduled, 0 = scheduled, 1 = running, 2 = updated while running
  private final AtomicInteger taskState;
  private final CheckRunner runner;
  private volatile long scheduleDelay;
  
  /**
   * Construct a new operation with an executor to execute on.  Because this takes an executor and 
   * not a scheduler an {@link UnsupportedOperationException} will be thrown if 
   * {@link #setScheduleDelay(long)} updates the schedule to be anything non-zero. 
   * 
   * @since 5.15
   * @param executor Executor to execute on
   */
  protected ReschedulingOperation(Executor executor) {
    this(executor, null, 0);
    
    ArgumentVerifier.assertNotNull(executor, "executor");
  }
  
  /**
   * Construct a new operation with the provided scheduler to schedule on to and the initial delay.
   * 
   * @param scheduler Scheduler to execute on.
   * @param scheduleDelay Delay in milliseconds to schedule operation out when has stuff to do
   */
  protected ReschedulingOperation(SubmitterScheduler scheduler, long scheduleDelay) {
    this(scheduler, scheduler, scheduleDelay);
    
    ArgumentVerifier.assertNotNull(scheduler, "scheduler");
  }
  
  private ReschedulingOperation(Executor executor, SubmitterScheduler scheduler, long scheduleDelay) {
    ArgumentVerifier.assertNotNegative(scheduleDelay, "scheduleDelay");
    
    this.executor = executor;
    this.scheduler = scheduler;
    this.taskState = new AtomicInteger(-1);
    this.runner = new CheckRunner();
    this.scheduleDelay = scheduleDelay;
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
    if (scheduler == null && scheduleDelay != 0) {
      throw new UnsupportedOperationException("Only an executor is provided, scheduling not possible");
    } else {
      ArgumentVerifier.assertNotNegative(scheduleDelay, "scheduleDelay");
        
      this.scheduleDelay = scheduleDelay;
    }
  }
  
  private boolean firstSignal() {
    while (true) {
      int casState = taskState.get();
      if (casState == -1) {
        if (taskState.compareAndSet(-1, 0)) {
          return true;
        }
      } else if (casState == 1) {
        if (taskState.compareAndSet(1, 2)) {
          return false;
        }
      } else {
        // either already scheduled, or already marked as more added
        return false;
      }
    }
  }
  
  /**
   * Similar to {@link #signalToRun()} except that any configured schedule / delay will be ignored 
   * and instead the task will try to run ASAP.
   * 
   * @param runOnCallingThreadIfPossible {@code true} to run the task on the invoking thread if possible
   */
  public void signalToRunImmediately(boolean runOnCallingThreadIfPossible) {
    if (firstSignal()) {
      if (runOnCallingThreadIfPossible) {
        runner.run();
      } else {
        executor.execute(runner);
      }
    }
  }
  
  /**
   * Invoke to indicate that this operation has stuff to do.  If necessary the task will be 
   * scheduled for execution.  If the task is already running then it will ensure the task 
   * re-executes itself when done (at the set delay).  This re-execution can help ensure that any 
   * thread state changes can be witnessed on the next execution.
   * <p>
   * If you want to signal the task to run immediately (ignore the schedule delay) please see 
   * {@link #signalToRunImmediately(boolean)}.
   */
  public void signalToRun() {
    if (firstSignal()) {
      executeRunner();
    }
  }
  
  private void executeRunner() {
    if (scheduler != null) {
      scheduler.schedule(runner, scheduleDelay);
    } else {
      executor.execute(runner);
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
              // set back to idle state, we are done
              break;
            }
          } else if (taskState.get() == 2) { // will be set back to 1 when this restarts
            executeRunner();
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
