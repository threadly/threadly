package org.threadly.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * A thread pool for scheduling tasks with provided futures.  
 * This scheduler submits tasks and returns futures for when they will be executed.
 * 
 * @author jent - Mike Jensen
 */
public interface SubmitterSchedulerInterface extends SimpleSchedulerInterface {
  /**
   * Submit a task to run as soon as possible.  There is a 
   * slight increase in load when using submit over execute.  
   * So this should only be used when the future is necessary.
   * 
   * The future .get() method will return null once the runnable has completed.
   * 
   * @param task runnable to be executed
   * @return a future to know when the task has completed
   */
  public Future<?> submit(Runnable task);
  
  /**
   * Submit a task to run as soon as possible.  There is a 
   * slight increase in load when using submit over execute.  
   * So this should only be used when the future is necessary.
   * 
   * The future .get() method will return null once the runnable has completed.
   * 
   * @param task runnable to be executed
   * @param result result to be returned from resulting future .get() when runnable completes
   * @return a future to know when the task has completed
   */
  public <T> Future<T> submit(Runnable task, T result);

  /**
   * Submit a {@link Callable} to run as soon as possible.  This is 
   * needed when a result needs to be consumed from the 
   * callable.
   * 
   * @param task callable to be executed
   * @return a future to know when the task has completed and get the result of the callable
   */
  public <T> Future<T> submit(Callable<T> task);
  
  /**
   * Schedule a task with a given delay.  There is a slight 
   * increase in load when using submitScheduled over schedule.  So 
   * this should only be used when the future is necessary.
   * 
   * The future .get() method will return null once the runnable has completed.
   * 
   * @param task runnable to execute
   * @param delayInMs time in milliseconds to wait to execute task
   * @return a future to know when the task has completed
   */
  public Future<?> submitScheduled(Runnable task, 
                                   long delayInMs);
  
  /**
   * Schedule a task with a given delay.  There is a slight 
   * increase in load when using submitScheduled over schedule.  So 
   * this should only be used when the future is necessary.
   * 
   * The future .get() method will return null once the runnable has completed.
   * 
   * @param task runnable to execute
   * @param result result to be returned from resulting future .get() when runnable completes
   * @param delayInMs time in milliseconds to wait to execute task
   * @return a future to know when the task has completed
   */
  public <T> Future<T> submitScheduled(Runnable task, T result,  
                                       long delayInMs);
  
  /**
   * Schedule a {@link Callable} with a given delay.  This is 
   * needed when a result needs to be consumed from the 
   * callable.
   * 
   * @param task callable to be executed
   * @param delayInMs time in milliseconds to wait to execute task
   * @return a future to know when the task has completed and get the result of the callable
   */
  public <T> Future<T> submitScheduled(Callable<T> task, 
                                       long delayInMs);
}
