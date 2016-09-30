package org.threadly.concurrent.wrapper.compatibility;

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

import org.threadly.concurrent.RunnableContainer;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.future.FutureUtils;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableScheduledFuture;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;

/**
 * <p>Generic implementation for the wrappers that implement {@link ScheduledExecutorService}.  
 * This allows us to add new wrappers with the minimal amount of duplicated code.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.6.0 (since 2.0.0 at org.threadly.concurrent)
 */
abstract class AbstractExecutorServiceWrapper implements ScheduledExecutorService {
  protected final SchedulerService scheduler;
  
  /**
   * Constructs a new wrapper to adhere to the {@link ScheduledExecutorService} interface.
   * 
   * @param scheduler scheduler implementation to rely on
   */
  public AbstractExecutorServiceWrapper(SchedulerService scheduler) {
    ArgumentVerifier.assertNotNull(scheduler, "scheduler");
    
    this.scheduler = scheduler;
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
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
    long timeoutInMs = unit.toMillis(timeout);
    long startTime = timeoutInMs < Long.MAX_VALUE ? Clock.accurateForwardProgressingMillis() : -1;
    List<Future<T>> resultList = new ArrayList<>(tasks.size());
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
    if (timeoutInMs < Long.MAX_VALUE) {
      long remainingTime = timeoutInMs - (Clock.accurateForwardProgressingMillis() - startTime);
      try {
        FutureUtils.blockTillAllComplete(resultList, remainingTime);
      } catch (TimeoutException e) {
        FutureUtils.cancelIncompleteFutures(resultList, true);
      }
    } else {
      FutureUtils.blockTillAllComplete(resultList);
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
    if (tasks.size() < 1) {
      throw new IllegalArgumentException("Empty task list provided");
    }
    
    final long startTime = Clock.accurateForwardProgressingMillis();
    final long timeoutInMs = unit.toMillis(timeout);
    int failureCount = 0;
    // going to be optimistic and allocate the initialize size so that at most we have to do one expansion
    List<Future<T>> submittedFutures = new ArrayList<>((tasks.size() / 2) + 1);
    
    try {
      ExecutorCompletionService<T> ecs = new ExecutorCompletionService<>(this);
      ExecutionException lastEE = null;
      Iterator<? extends Callable<T>> it = tasks.iterator();
      // submit first one
      submittedFutures.add(ecs.submit(it.next()));

      long remainingTime = timeoutInMs - (Clock.lastKnownForwardProgressingMillis() - startTime);
      while (it.hasNext() && remainingTime > 0) {
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
        remainingTime = timeoutInMs - (Clock.accurateForwardProgressingMillis() - startTime);
      }
      
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
        remainingTime = timeoutInMs - (Clock.accurateForwardProgressingMillis() - startTime);
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
      FutureUtils.cancelIncompleteFutures(submittedFutures, true);
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
    } else if (delay <= 0) {
      throw new IllegalArgumentException();
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

  @Override
  public ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable task,
                                                          long initialDelay, long period,
                                                          TimeUnit unit) {
    if (task == null) {
      throw new NullPointerException("Must provide task");
    } else if (period <= 0) {
      throw new IllegalArgumentException();
    } else if (initialDelay < 0) {
      initialDelay = 0;
    }
    
    long initialDelayInMs = unit.toMillis(initialDelay);
    long periodInMs = unit.toMillis(period);
    
    return scheduleAtFixedRate(task, initialDelayInMs, periodInMs);
  }
  
  protected abstract ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable task,
                                                                      long initialDelayInMillis,
                                                                      long periodInMillis);
  
  /**
   * <p>Because in {@link java.util.concurrent.ScheduledExecutorService} an exception from a 
   * recurring task causes the task to stop executing, we have to wrap the task.  That way we can 
   * remove the recurring task if the error occurs (since 
   * {@link org.threadly.concurrent.SubmitterScheduler} will continue to execute the task despite 
   * the error.</p>
   * 
   * @author jent - Mike Jensen
   * @since 2.1.0
   */
  protected static class ThrowableHandlingRecurringRunnable implements RunnableContainer, Runnable {
    private final SchedulerService scheduler;
    private final Runnable task;
    
    protected ThrowableHandlingRecurringRunnable(SchedulerService scheduler, Runnable task) {
      this.scheduler = scheduler;
      this.task = task;
    }
    
    @Override
    public void run() {
      try {
        task.run();
      } catch (Throwable t) {
        scheduler.remove(this);
        ExceptionUtils.handleException(t);
      }
    }

    @Override
    public Runnable getContainedRunnable() {
      return task;
    }
  }
  
  /**
   * <p>An implementation of {@link ListenableFutureTask} which will remove the task from the 
   * scheduler when cancel is invoked.  Threadly does not normally have this behavior for a 
   * couple reasons.  Because we don't return futures on recurring tasks, canceling a future just 
   * results in a one time task execution that is a quick no-op.  It is cheaper in threadly to 
   * allow this no-op task on .cancel than to attempt removal.  Because 
   * {@link ScheduledExecutorService} returns a future that can be canceled for recurring tasks, 
   * we want to go ahead and remove the task (rather than have recurring no-op executions).</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.4.3
   * @param <T> The result object type returned by this future
   */
  protected static class CancelRemovingListenableFutureTask<T> extends ListenableFutureTask<T> {
    private final SchedulerService scheduler;

    public CancelRemovingListenableFutureTask(SchedulerService scheduler, 
                                              boolean recurring, Runnable task) {
      super(recurring, task);
      
      this.scheduler = scheduler;
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (super.cancel(mayInterruptIfRunning)) {
        scheduler.remove(this.getContainedCallable());
        return true;
      } else {
        return false;
      }
    }
  }
}
