package org.threadly.concurrent;

import java.util.concurrent.Executor;

public interface SimpleSchedulerInterface extends Executor {
  public void scheduleWithFixedDelay(Runnable task, 
                                     long initialDelay, 
                                     long recurringDelay);
  
  public void schedule(Runnable task, 
                       long delayInMs);
}
