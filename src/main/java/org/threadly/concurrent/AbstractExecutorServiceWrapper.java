package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableScheduledFuture;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;

/**
 * <p>Generic implementation for the wrappers that implement {@link ScheduledExecutorService}.  
 * This allows us to add new wrappers with the minimal amount of duplicated code.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
abstract class AbstractExecutorServiceWrapper implements ScheduledExecutorService {
  private static final int AWAIT_TERMINATION_POLL_INTERVAL_IN_NANOS = 1000000 * 100;  // 100ms
  
  private final SchedulerServiceInterface scheduler;
  private final TaskExecutorDistributor fixedRateDistributor;
  
  /**
   * Constructs a new wrapper to adhere to the {@link ScheduledExecutorService} interface.
   * 
   * @param scheduler scheduler implementation to rely on
   */
  public AbstractExecutorServiceWrapper(SchedulerServiceInterface scheduler) {
    ArgumentVerifier.assertNotNull(scheduler, "scheduler");
    
    this.scheduler = scheduler;
    this.fixedRateDistributor = new TaskExecutorDistributor(scheduler);
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }

  @Override
  public boolean awaitTermination(long timeout, 
                                  TimeUnit unit) {
    long startTime = Clock.accurateTimeMillis();
    long waitTimeInMs = unit.toMillis(timeout);
    Thread currentThread = Thread.currentThread();
    while (! isTerminated() && 
           Clock.accurateTimeMillis() - startTime < waitTimeInMs && 
           ! currentThread.isInterrupted()) {
      // just spin till terminated or time expires
      LockSupport.parkNanos(AWAIT_TERMINATION_POLL_INTERVAL_IN_NANOS);
    }
    
    return isTerminated();
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    return scheduler.submit(task);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result) {
    return scheduler.submit(task, result);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task) {
    return scheduler.submit(task);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
    return invokeAll(tasks, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
                                       long timeout, TimeUnit unit) throws InterruptedException {
    long startTime = Clock.accurateTimeMillis();
    long timeoutInMs = unit.toMillis(timeout);
    List<Future<T>> resultList = new ArrayList<Future<T>>(tasks.size());
    // execute all the tasks provided
    {
      Iterator<? extends Callable<T>> it = tasks.iterator();
      while (it.hasNext()) {
        Callable<T> c = it.next();
        if (c == null) {
          throw new NullPointerException();
        }
        
        ListenableFuture<T> lf = scheduler.submit(c);
        resultList.add(lf);
      }
    }
    // block till all tasks finish, or we reach our timeout
    {
      Iterator<Future<T>> it = resultList.iterator();
      long remainingTime = timeoutInMs - (Clock.accurateTimeMillis() - startTime); 
      while (it.hasNext() && remainingTime > 0) {
        Future<T> f = it.next();
        try {
          f.get(remainingTime, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
          // ignored here
        } catch (TimeoutException e) {
          f.cancel(true);
          break;
        }
        remainingTime = timeoutInMs - (Clock.accurateTimeMillis() - startTime); 
      }
      // cancel any which have not completed yet (assuming they are not done)
      while (it.hasNext()) {
        it.next().cancel(true);
      }
    }
    
    return resultList;
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException,
                                                                         ExecutionException {
    try {
      return invokeAny(tasks, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      // basically impossible
      throw ExceptionUtils.makeRuntime(e);
    }
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, 
                         long timeout, TimeUnit unit) throws InterruptedException,
                                                             ExecutionException, 
                                                             TimeoutException {
    final long startTime = Clock.accurateTimeMillis();
    if (tasks.size() < 1) {
      throw new IllegalArgumentException("Empty task list provided");
    }
    final long timeoutInMs = unit.toMillis(timeout);
    int failureCount = 0;
    // going to be optimistic and allocate the initialize size so that at most we have to do one expansion
    List<Future<T>> submittedFutures = new ArrayList<Future<T>>((tasks.size() / 2) + 1);
    
    try {
      ExecutorCompletionService<T> ecs = new ExecutorCompletionService<T>(this);
      ExecutionException lastEE = null;
      Iterator<? extends Callable<T>> it = tasks.iterator();
      // submit first one
      submittedFutures.add(ecs.submit(it.next()));
      
      while (it.hasNext() && 
             Clock.accurateTimeMillis() - startTime < timeoutInMs) {
        Future<T> completedFuture = ecs.poll();
        if (completedFuture == null) {
          // submit another
          submittedFutures.add(ecs.submit(it.next()));
        } else {
          try {
            return completedFuture.get();
          } catch (ExecutionException e) {
            failureCount++;
            lastEE = e;
          }
        }
      }
      
      long remainingTime = timeoutInMs - (Clock.lastKnownTimeMillis() - startTime);
      // we must compare against failure count otherwise we may throw a TimeoutException when all tasks have failed
      while (remainingTime > 0 && failureCount < submittedFutures.size()) {
        Future<T> completedFuture = ecs.poll(remainingTime, TimeUnit.MILLISECONDS);
        if (completedFuture == null) {
          throw new TimeoutException();
        } else {
          try {
            return completedFuture.get();
          } catch (ExecutionException e) {
            failureCount++;
            lastEE = e;
          }
        }
        
        remainingTime = timeoutInMs - (Clock.accurateTimeMillis() - startTime);
      }
      
      if (remainingTime <= 0) {
        throw new TimeoutException();
      } else {
        /* since we know we have at least one task provided, and since nothing returned by this point
         * we know that we only got ExecutionExceptions, and thus this should NOT be null
         */
        throw lastEE;
      }
    } finally {
      Iterator<Future<T>> it = submittedFutures.iterator();
      while (it.hasNext()) {
        it.next().cancel(true);
      }
    }
  }

  @Override
  public void execute(Runnable task) {
    scheduler.execute(task);
  }
  
  @Override
  public ListenableScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
    if (task == null) {
      throw new NullPointerException("Must provide task");
    } else if (delay < 0) {
      delay = 0;
    }
    
    return schedule(task, unit.toMillis(delay));
  }

  protected abstract ListenableScheduledFuture<?> schedule(Runnable task, long delayInMillis);

  @Override
  public <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delay,
                                                   TimeUnit unit) {
    if (callable == null) {
      throw new NullPointerException("Must provide task");
    } else if (delay < 0) {
      delay = 0;
    }
    
    return schedule(callable, unit.toMillis(delay));
  }
  

  protected abstract <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delayInMillis);
  
  @Override
  public ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable task,
                                                             long initialDelay,
                                                             long delay, TimeUnit unit) {
    if (task == null) {
      throw new NullPointerException("Must provide task");
    } else if (delay < 0) {
      delay = 0;
    } else if (initialDelay < 0) {
      initialDelay = 0;
    }
    
    long initialDelayInMs = unit.toMillis(initialDelay);
    long delayInMs = unit.toMillis(delay);
    
    return scheduleWithFixedDelay(task, initialDelayInMs, delayInMs);
  }
  
  protected abstract ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable task,
                                                                         long initialDelayInMillis,
                                                                         long delayInMillis);

  /**
   * USE WITH CAUTION
   * 
   * Although I have implemented this as close to the javaspec as possible, this is a bit of a 
   * hack.  For the longest time this just threw an UnsupportedOperationException, I did not want 
   * to implement it because the {@link PriorityScheduler} is just not designed to handle this.
   * 
   * The largest problem with the Threadly implementation of this is that the rate may not be 
   * as precise as it was in the java implementation.  In addition it is less efficient and wont 
   * get the same throughput as with the java.util.concurrent implementation.
   *
   * From the open jdk javadoc....
   * Creates and executes a periodic action that becomes enabled first
   * after the given initial delay, and subsequently with the given
   * period; that is executions will commence after
   * <tt>initialDelay</tt> then <tt>initialDelay+period</tt>, then
   * <tt>initialDelay + 2 * period</tt>, and so on.
   * If any execution of the task
   * encounters an exception, subsequent executions are suppressed.
   * Otherwise, the task will only terminate via cancellation or
   * termination of the executor.  If any execution of this task
   * takes longer than its period, then subsequent executions
   * may start late, but will not concurrently execute.
   *
   * @param task the task to execute
   * @param initialDelay the time to delay first execution
   * @param period the period between successive executions
   * @param unit the time unit of the initialDelay and period parameters
   * @return a ScheduledFuture representing pending completion of
   *         the task, and whose <tt>get()</tt> method will throw an
   *         exception upon cancellation
   * @throws NullPointerException if command is null
   * @throws IllegalArgumentException if period less than or equal to zero
   */
  @Override
  public ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable task,
                                                          long initialDelay, long period,
                                                          TimeUnit unit) {
    if (task == null) {
      throw new NullPointerException("Must provide task");
    }
    ArgumentVerifier.assertGreaterThanZero(period, "period");
    
    if (initialDelay < 0) {
      initialDelay = 0;
    }
    
    long initialDelayInMs = unit.toMillis(initialDelay);
    long periodInMs = unit.toMillis(period);

    // first wrap the task to prevent execution if exception has thrown
    FixedRateTaskWrapper wrappedTask = new FixedRateTaskWrapper(task);
    /* then create a task to submit the task to the distributor.  The distributor ensures
     * that this wont run concurrently.  This will also have the job of removing itself 
     * from the scheduler if an exception was thrown.
     */
    FixedRateSubmitter frs = new FixedRateSubmitter(scheduler, fixedRateDistributor, wrappedTask);
    
    return scheduleWithFixedDelay(frs, initialDelayInMs, periodInMs);
  }
  
  /**
   * <p>This classes job is to run a task, if an exception was thrown running the task 
   * previously, it wont attempt to run the task again.</p>
   * 
   * <p>It is expected that this does NOT run concurrently, but may run on different threads.</p>
   * 
   * @author jent - Mike Jensen
   * @since 2.0.0
   */
  protected static class FixedRateTaskWrapper implements Runnable, RunnableContainerInterface {
    private final Runnable originalTask;
    private volatile boolean exceptionThrown;
    
    protected FixedRateTaskWrapper(Runnable originalTask) {
      this.originalTask = originalTask;
      this.exceptionThrown = false;
    }
    
    public boolean exceptionThrown() {
      return exceptionThrown;
    }
    
    @Override
    public Runnable getContainedRunnable() {
      return originalTask;
    }

    @Override
    public void run() {
      if (! exceptionThrown) {
        try {
          originalTask.run();
        } catch (Throwable t) {
          exceptionThrown = true;
          throw ExceptionUtils.makeRuntime(t);
        }
      }
    }
  }
  
  /**
   * <p>This class is what is responsible for submitting the task back into the taskDistributor.  
   * It is very short lived, and thus is what is ensuring the regular rate.  It also has the 
   * responsibility of removing itself from the constant execution if the wrapped task did throw 
   * an exception</p>
   * 
   * @author jent - Mike Jensen
   * @since 2.0.0
   */
  protected static class FixedRateSubmitter implements Runnable, RunnableContainerInterface {
    private final SchedulerServiceInterface scheduler;
    private final TaskExecutorDistributor fixedRateDistributor;
    private final FixedRateTaskWrapper wrappedTask;
    
    protected FixedRateSubmitter(SchedulerServiceInterface scheduler, 
                                 TaskExecutorDistributor fixedRateDistributor, 
                                 FixedRateTaskWrapper wrappedTask) {
      this.scheduler = scheduler;
      this.fixedRateDistributor = fixedRateDistributor;
      this.wrappedTask = wrappedTask;
    }

    @Override
    public Runnable getContainedRunnable() {
      return wrappedTask.getContainedRunnable();
    }

    @Override
    public void run() {
      if (wrappedTask.exceptionThrown()) {
        scheduler.remove(this); // no need to keep attempting to execute
      } else {
        fixedRateDistributor.addTask(wrappedTask.getContainedRunnable(), wrappedTask);
      }
    }
  }
  
  /**
   * <p>Because in {@link java.util.concurrent.ScheduledExecutorService} an exception from a 
   * recurring task causes the task to stop executing, we have to wrap the task.  That way 
   * we can remove the recurring task if the error occurs (since {@link SimpleSchedulerInterface} 
   * will continue to execute the task despite the error.</p>
   * 
   * @author jent - Mike Jensen
   * @since 2.1.0
   */
  protected static class ThrowableHandlingRecurringRunnable implements RunnableContainerInterface, Runnable {
    private final SchedulerServiceInterface scheduler;
    private final Runnable task;
    
    protected ThrowableHandlingRecurringRunnable(SchedulerServiceInterface scheduler, Runnable task) {
      this.scheduler = scheduler;
      this.task = task;
    }
    
    @Override
    public void run() {
      try {
        task.run();
      } catch (Throwable t) {
        ExceptionUtils.handleException(t);
        scheduler.remove(this);
      }
    }

    @Override
    public Runnable getContainedRunnable() {
      return task;
    }
  }
}
