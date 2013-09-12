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

import org.threadly.concurrent.PriorityScheduledExecutor.OneTimeTaskWrapper;
import org.threadly.concurrent.PriorityScheduledExecutor.RecurringTaskWrapper;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.concurrent.future.ListenableScheduledFuture;
import org.threadly.concurrent.future.ScheduledFutureDelegate;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;

/**
 * This is a wrapper for {@link PriorityScheduledExecutor} to be a drop in replacement
 * for any {@link ScheduledExecutorService} (aka the {@link java.util.concurrent.ScheduledThreadPoolExecutor} 
 * interface). It does make some performance sacrifices to adhere to this interface, but those
 * are pretty minimal.
 * 
 * @author jent - Mike Jensen
 */
public class PriorityScheduledExecutorServiceWrapper implements ScheduledExecutorService {
  private static final int AWAIT_TERMINATION_POLL_INTERVAL_IN_NANOS = 1000000 * 100;  // 100ms
  
  private final PriorityScheduledExecutor scheduler;
  
  /**
   * Constructs a new wrapper to adhere to the {@link ScheduledExecutorService} interface.
   * 
   * @param scheduler scheduler implementation to rely on
   */
  public PriorityScheduledExecutorServiceWrapper(PriorityScheduledExecutor scheduler) {
    if (scheduler == null) {
      throw new IllegalArgumentException("Must provide scheduler implementation");
    }
    
    this.scheduler = scheduler;
  }

  @Override
  public void shutdown() {
    scheduler.shutdown();
  }

  /**
   * This call will stop the processor as quick as possible.  Any 
   * tasks which are awaiting execution will be canceled and returned 
   * as a result to this call.
   * 
   * Unlike java.util.concurrent.ExecutorService implementation there 
   * is no attempt to stop any currently execution tasks.
   *
   * This method does not wait for actively executing tasks to
   * terminate.  Use {@link #awaitTermination awaitTermination} to
   * do that.
   *
   * @return list of tasks that never commenced execution
   */
  @Override
  public List<Runnable> shutdownNow() {
    return scheduler.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return scheduler.isShutdown() && 
           scheduler.getCurrentPoolSize() == 0;
  }

  @Override
  public boolean awaitTermination(long timeout, 
                                  TimeUnit unit) {
    long startTime = Clock.accurateTime();
    long waitTimeInMs = TimeUnit.MILLISECONDS.convert(timeout, unit);
    while (! isTerminated() && 
           Clock.accurateTime() - startTime < waitTimeInMs) {
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
    long startTime = Clock.accurateTime();
    long timeoutInMs = TimeUnit.MILLISECONDS.convert(timeout, unit);
    List<Future<T>> resultList = new ArrayList<Future<T>>(tasks.size());
    {
      Iterator<? extends Callable<T>> it = tasks.iterator();
      while (it.hasNext()) {
        Callable<T> c = it.next();
        if (c == null) {
          throw new NullPointerException();
        }
        
        ListenableRunnableFuture<T> fr = new ListenableFutureTask<T>(false, c);
        resultList.add(fr);
        scheduler.execute(fr);
      }
    }
    {
      Iterator<Future<T>> it = resultList.iterator();
      long remainingTime = Math.max(0, timeoutInMs - (Clock.accurateTime() - startTime)); 
      while (it.hasNext() && remainingTime > 0) {
        try {
          it.next().get(remainingTime, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
          // ignored here
        } catch (TimeoutException e) {
          break;
        }
        remainingTime = Math.max(0, timeoutInMs - (Clock.accurateTime() - startTime)); 
      }
      // cancel any which have not completed yet
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
    final long timeoutInMs = TimeUnit.MILLISECONDS.convert(timeout, unit);
    final long startTime = Clock.accurateTime();
    List<Future<T>> futures = new ArrayList<Future<T>>(tasks.size());
    ExecutorCompletionService<T> ecs = new ExecutorCompletionService<T>(this);
    
    try {
      Iterator<? extends Callable<T>> it = tasks.iterator();
      if (it.hasNext()) {
        // submit first one
        Future<T> submittedFuture = ecs.submit(it.next());
        futures.add(submittedFuture);
      }
      Future<T> completedFuture = null;
      while (completedFuture == null && it.hasNext() && 
             Clock.accurateTime() - startTime < timeoutInMs) {
        completedFuture = ecs.poll();
        if (completedFuture == null) {
          // submit another
          futures.add(ecs.submit(it.next()));
        } else {
          return completedFuture.get();
        }
      }
      
      if (Clock.lastKnownTimeMillis() - startTime >= timeoutInMs) {
        throw new TimeoutException();
      } else {
        long remainingTime = timeoutInMs - (Clock.lastKnownTimeMillis() - startTime);
        completedFuture = ecs.poll(remainingTime, TimeUnit.MILLISECONDS);
        if (completedFuture == null) {
          throw new TimeoutException();
        } else {
          return completedFuture.get();
        }
      }
    } finally {
      Iterator<Future<T>> it = futures.iterator();
      while (it.hasNext()) {
        it.next().cancel(true);
      }
    }
  }

  @Override
  public void execute(Runnable command) {
    scheduler.execute(command);
  }

  @Override
  public ListenableScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    if (command == null) {
      throw new NullPointerException("Must provide a task");
    } else if (delay < 0) {
      delay = 0;
    }
    
    long delayInMs = TimeUnit.MILLISECONDS.convert(delay, unit);

    ListenableRunnableFuture<Object> taskFuture = new ListenableFutureTask<Object>(false, command);
    OneTimeTaskWrapper ottw = new OneTimeTaskWrapper(taskFuture, 
                                                     scheduler.getDefaultPriority(), 
                                                     delayInMs);
    scheduler.addToQueue(ottw);
    
    return new ScheduledFutureDelegate<Object>(taskFuture, ottw);
  }

  @Override
  public <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delay,
                                                   TimeUnit unit) {
    if (callable == null) {
      throw new NullPointerException("Must provide a task");
    } else if (delay < 0) {
      delay = 0;
    }
    
    long delayInMs = TimeUnit.MILLISECONDS.convert(delay, unit);

    ListenableRunnableFuture<V> taskFuture = new ListenableFutureTask<V>(false, callable);
    OneTimeTaskWrapper ottw = new OneTimeTaskWrapper(taskFuture, 
                                                     scheduler.getDefaultPriority(), 
                                                     delayInMs);
    scheduler.addToQueue(ottw);
    
    return new ScheduledFutureDelegate<V>(taskFuture, ottw);
  }

  /**
   * Not implemented yet, will always throw UnsupportedOperationException.
   * 
   * throws UnsupportedOperationException not yet implemented
   */
  @Override
  public ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                          long initialDelay, long period,
                                                          TimeUnit unit) {
    throw new UnsupportedOperationException("Not implemented in wrapper");
  }

  @Override
  public ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                             long initialDelay,
                                                             long delay, TimeUnit unit) {
    if (command == null) {
      throw new NullPointerException("Must provide a task");
    } else if (delay < 0) {
      delay = 0;
    } else if (initialDelay < 0) {
      initialDelay = 0;
    }
    
    long initialDelayInMs = TimeUnit.MILLISECONDS.convert(delay, unit);
    long delayInMs = TimeUnit.MILLISECONDS.convert(delay, unit);

    ListenableRunnableFuture<Object> taskFuture = new ListenableFutureTask<Object>(true, command);
    RecurringTaskWrapper rtw = scheduler.new RecurringTaskWrapper(taskFuture, 
                                                                  scheduler.getDefaultPriority(), 
                                                                  initialDelayInMs, delayInMs);
    scheduler.addToQueue(rtw);
    
    return new ScheduledFutureDelegate<Object>(taskFuture, rtw);
  }
}
