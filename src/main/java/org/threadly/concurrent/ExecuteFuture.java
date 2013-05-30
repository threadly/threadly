package org.threadly.concurrent;

import java.util.concurrent.ExecutionException;

/**
 * A simplified Future designed for tasks which have no results.
 * 
 * @author jent - Mike Jensen
 */
public interface ExecuteFuture {
  /**
   * Blocks until the task has completed.  If the task has 
   * already finished, this returns immediately.
   * @throws ExecutionException thrown if the runnable threw an exception.  The cause is the runnable's exception.
   * @throws InterruptedException exception thrown if thread is interrupted while blocking.
   */
  public void blockTillCompleted() throws InterruptedException, ExecutionException;
  
  /**
   * @return true if the executed task has finished running
   */
  public boolean isCompleted();
}
