package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

import org.threadly.concurrent.PriorityScheduledExecutor.OneTimeTaskWrapper;
import org.threadly.concurrent.PriorityScheduledExecutor.RecurringTaskWrapper;
import org.threadly.concurrent.PriorityScheduledExecutor.TaskWrapper;
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
   * Currently the {@link PriorityScheduledExecutor} does not track
   * tasks while they are running, so this call will always
   * return an empty list of runnables it was able to terminate.
   */
  @Override
  public List<Runnable> shutdownNow() {
    scheduler.shutdown();
    
    return new ArrayList<Runnable>(0);
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
                                  TimeUnit unit) throws InterruptedException {
    long startTime = Clock.accurateTime();
    long waitTimeInMs = TimeUnit.MILLISECONDS.convert(timeout, unit);
    while (! isTerminated() && 
           Clock.accurateTime() - startTime < waitTimeInMs) {
      LockSupport.parkNanos(AWAIT_TERMINATION_POLL_INTERVAL_IN_NANOS);
    }
    
    return isTerminated();
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    FutureRunnable<T> fr = new FutureRunnable<T>(task);
    
    scheduler.execute(fr);
    
    return fr;
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    FutureRunnable<T> fr = new FutureRunnable<T>(task, result);
    
    scheduler.execute(fr);
    
    return fr;
  }

  @Override
  public Future<?> submit(Runnable task) {
    FutureRunnable<?> fr = new FutureRunnable<Object>(task);
    
    scheduler.execute(fr);
    
    return fr;
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
        
        FutureRunnable<T> fr = new FutureRunnable<T>(c);
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
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    if (command == null) {
      throw new NullPointerException("Must provide a task");
    } else if (delay < 0) {
      delay = 0;
    }
    
    long delayInMs = TimeUnit.MILLISECONDS.convert(delay, unit);

    FutureRunnable<Object> taskFuture = new FutureRunnable<Object>(command);
    OneTimeTaskWrapper ottw = new OneTimeTaskWrapper(taskFuture, 
                                                     scheduler.getDefaultPriority(), 
                                                     delayInMs);
    scheduler.addToQueue(ottw);
    
    return new ScheduledFutureRunnable<Object>(taskFuture, ottw);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay,
                                         TimeUnit unit) {
    if (callable == null) {
      throw new NullPointerException("Must provide a task");
    } else if (delay < 0) {
      delay = 0;
    }
    
    long delayInMs = TimeUnit.MILLISECONDS.convert(delay, unit);

    FutureRunnable<V> taskFuture = new FutureRunnable<V>(callable);
    OneTimeTaskWrapper ottw = new OneTimeTaskWrapper(taskFuture, 
                                                     scheduler.getDefaultPriority(), 
                                                     delayInMs);
    scheduler.addToQueue(ottw);
    
    return new ScheduledFutureRunnable<V>(taskFuture, ottw);
  }

  /**
   * Not implemented yet, will always throw UnsupportedOperationException.
   * 
   * throws UnsupportedOperationException not yet implemented
   */
  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                long initialDelay, long period,
                                                TimeUnit unit) {
    throw new UnsupportedOperationException("Not implemented in wrapper");
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
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

    FutureRunnable<Object> taskFuture = new FutureRunnable<Object>(command);
    RecurringTaskWrapper rtw = scheduler.new RecurringTaskWrapper(taskFuture, 
                                                                  scheduler.getDefaultPriority(), 
                                                                  initialDelayInMs, delayInMs);
    scheduler.addToQueue(rtw);
    
    return new ScheduledFutureRunnable<Object>(taskFuture, rtw);
  }
  
  /**
   * FutureRunnable to be returned for scheduled calls.
   * 
   * @author jent - Mike Jensen
   *
   * @param <T> generic for callable result
   */
  protected class FutureRunnable<T> extends VirtualRunnable 
                                    implements Future<T> {
    private final Callable<T> task;
    private final Runnable toRun;
    private volatile T result;
    private volatile Exception thrownException;
    private volatile boolean isCancelled;
    private boolean hasRun; // guarded by synchronization on this
    
    protected FutureRunnable(Callable<T> task) {
      this.task = task;
      this.toRun = null;
      this.result = null;
      thrownException = null;
      isCancelled = false;
      hasRun = false;
    }
    
    protected FutureRunnable(Runnable toRun) {
      this(toRun, null);
    }
    
    protected FutureRunnable(Runnable toRun, T result) {
      this.task = null;
      this.toRun = toRun;
      this.result = result;
      thrownException = null;
      isCancelled = false;
      hasRun = false;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      synchronized (this) {
        isCancelled = true;
        
        this.notifyAll();
        
        return ! hasRun;
      }
    }

    @Override
    public boolean isCancelled() {
      return isCancelled;
    }

    @Override
    public boolean isDone() {
      return hasRun;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      try {
        return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        // ignored, can't happen
        throw ExceptionUtils.makeRuntime(e);
      }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException,
                                                     ExecutionException,
                                                     TimeoutException {
      long startTime = Clock.accurateTime();
      long waitTimeInMs = TimeUnit.MILLISECONDS.convert(timeout, unit);
      synchronized (this) {
        long waitTime;
        while (! hasRun && ! isCancelled && 
               (waitTime = Math.max(0,  waitTimeInMs - (Clock.accurateTime() - startTime))) > 0) {
          this.wait(waitTime);
        }
        
        if (isCancelled) {
          throw new CancellationException();
        } else if (thrownException != null) {
          throw new ExecutionException(thrownException);
        } else if (! hasRun) {
          throw new TimeoutException();
        } 
      }
      
      return result;
    }

    @Override
    public void run() {
      boolean run = false;
      try {
        if (! isCancelled) {
          run = true;
          if (task != null) {
            result = task.call();
          } else {
            toRun.run();
          }
        }
      } catch (Exception e) {
        thrownException = e;
      } finally {
        synchronized (this) {
          if (run) {
            hasRun = true;
          }
          
          this.notifyAll();
        }
      }
    }
  }

  /**
   * ScheduledFutureRunnable to be returned for recurring scheduled calls.
   * 
   * @author jent - Mike Jensen
   *
   * @param <T> generic for callable result
   */
  protected class ScheduledFutureRunnable<T> implements ScheduledFuture<T> {
    private final FutureRunnable<T> taskFuture;
    private final TaskWrapper scheduledTask;
    
    public ScheduledFutureRunnable(FutureRunnable<T> taskFuture,
                                   TaskWrapper scheduledTask) {
      this.taskFuture = taskFuture;
      this.scheduledTask = scheduledTask;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return scheduledTask.getDelay(unit);
    }

    @Override
    public int compareTo(Delayed o) {
      return scheduledTask.compareTo(o);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return taskFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return taskFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
      return taskFuture.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      return taskFuture.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException,
                                                     ExecutionException,
                                                     TimeoutException {
      return taskFuture.get(timeout, unit);
    }
  }
}
