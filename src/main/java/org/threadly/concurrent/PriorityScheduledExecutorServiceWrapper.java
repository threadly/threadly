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
 * <p>This is a wrapper for {@link PriorityScheduledExecutor} to be a drop in replacement
 * for any {@link ScheduledExecutorService} (aka the {@link java.util.concurrent.ScheduledThreadPoolExecutor} 
 * interface). It does make some performance sacrifices to adhere to this interface, but those
 * are pretty minimal.  The largest compromise in here is easily scheduleAtFixedRate (which you should 
 * read the javadocs for if you need).</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class PriorityScheduledExecutorServiceWrapper implements ScheduledExecutorService {
  private static final int AWAIT_TERMINATION_POLL_INTERVAL_IN_NANOS = 1000000 * 100;  // 100ms
  
  private final PriorityScheduledExecutor scheduler;
  private final TaskExecutorDistributor fixedRateDistributor;
  
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
    this.fixedRateDistributor = new TaskExecutorDistributor(scheduler);
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
    long waitTimeInMs = unit.toMillis(timeout);
    Thread currentThread = Thread.currentThread();
    while (! isTerminated() && 
           Clock.accurateTime() - startTime < waitTimeInMs && 
           ! currentThread.isInterrupted()) {
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
    long timeoutInMs = unit.toMillis(timeout);
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
    final long timeoutInMs = unit.toMillis(timeout);
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
    
    long delayInMs = unit.toMillis(delay);

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
    
    long delayInMs = unit.toMillis(delay);

    ListenableRunnableFuture<V> taskFuture = new ListenableFutureTask<V>(false, callable);
    OneTimeTaskWrapper ottw = new OneTimeTaskWrapper(taskFuture, 
                                                     scheduler.getDefaultPriority(), 
                                                     delayInMs);
    scheduler.addToQueue(ottw);
    
    return new ScheduledFutureDelegate<V>(taskFuture, ottw);
  }

  /**
   * USE WITH CAUTION
   * 
   * Although I have implemented this as close to the javaspec as possible, this is a bit of a 
   * hack.  For the longest time this just threw an UnsupportedOperationException, I did not want 
   * to implement it because the {@link PriorityScheduledExecutor} is just not designed to handle 
   * this.
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
   * @param command the task to execute
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
  public ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                          long initialDelay, long period,
                                                          TimeUnit unit) {
    if (command == null) {
      throw new NullPointerException("Must provide a task");
    } else if (period <= 0) {
      throw new IllegalArgumentException("period must be > 0");
    }
    
    if (initialDelay < 0) {
      initialDelay = 0;
    }
    
    long initialDelayInMs = unit.toMillis(initialDelay);
    long periodInMs = unit.toMillis(period);
    
    // first wrap the task to prevent execution if exception has thrown
    FixedRateTaskWrapper wrappedTask = new FixedRateTaskWrapper(command);
    /* then create a task to submit the task to the distributor.  The distributor ensures
     * that this wont run concurrently.  This will also have the job of removing itself 
     * from the scheduler if an exception was thrown.
     */
    FixedRateSubmitter frs = new FixedRateSubmitter(scheduler, fixedRateDistributor, wrappedTask);
    
    ListenableRunnableFuture<Object> taskFuture = new ListenableFutureTask<Object>(true, frs);
    RecurringTaskWrapper rtw = scheduler.new RecurringTaskWrapper(taskFuture, 
                                                                  scheduler.getDefaultPriority(), 
                                                                  initialDelayInMs, periodInMs);
    scheduler.addToQueue(rtw);
    
    return new ScheduledFutureDelegate<Object>(taskFuture, rtw);
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
    
    long initialDelayInMs = unit.toMillis(initialDelay);
    long delayInMs = unit.toMillis(delay);

    ListenableRunnableFuture<Object> taskFuture = new ListenableFutureTask<Object>(true, command);
    RecurringTaskWrapper rtw = scheduler.new RecurringTaskWrapper(taskFuture, 
                                                                  scheduler.getDefaultPriority(), 
                                                                  initialDelayInMs, delayInMs);
    scheduler.addToQueue(rtw);
    
    return new ScheduledFutureDelegate<Object>(taskFuture, rtw);
  }
  
  /**
   * <p>This classes job is to run a task, if an exception was thrown running the task 
   * previously, it wont attempt to run the task again.</p>
   * 
   * <p>It is expected that this does NOT run concurrently, but may run on different threads.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  private static class FixedRateTaskWrapper implements Runnable, RunnableContainerInterface {
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
   * @since 1.0.0
   */
  private static class FixedRateSubmitter implements Runnable, RunnableContainerInterface {
    private final PriorityScheduledExecutor scheduler;
    private final TaskExecutorDistributor fixedRateDistributor;
    private final FixedRateTaskWrapper wrappedTask;
    
    protected FixedRateSubmitter(PriorityScheduledExecutor scheduler, 
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
}
