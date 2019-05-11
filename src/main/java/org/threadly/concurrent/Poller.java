package org.threadly.concurrent;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.threadly.concurrent.future.FutureCallback;
import org.threadly.concurrent.future.FutureUtils;
import org.threadly.concurrent.future.ImmediateResultListenableFuture;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFuture.ListenerOptimizationStrategy;
import org.threadly.concurrent.future.ListenableFutureAdapterTask;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.concurrent.future.SettableListenableFuture;
import org.threadly.concurrent.future.Watchdog;
import org.threadly.util.ExceptionHandler;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.Pair;

/**
 * Simple class for watching a condition and getting notified when a state has changed.  One handy 
 * tool is the ability to transition java's {@link Future} into Threadly's much nicer 
 * {@link ListenableFuture}.
 * <p>
 * The frequency at which this polls should be determined based off how cheap polling is, and how 
 * many items will be polled on average.
 * <p>
 * If being allowed to garbage collect, this poller will continue to schedule itself as long as
 * there are outstanding futures.  Once all have completed (from timeout or naturally), then this
 * will stop scheduling itself to poll for updates.  Thus no explicit cleanup is needed.  As long 
 * as the {@link Supplier}'s are quick/fast ({@link Future} conversions are always quick/fast), 
 * it's most efficient to reuse the {@link Poller} instance.  But if you need dynamic 
 * timeout's/max wait time you could construct a {@link Poller} and toss it away once you get the 
 * returned {@link ListenableFuture} from it.
 * 
 * @since 5.0
 */
public class Poller {
  protected final SubmitterScheduler scheduler;
  private final Watchdog futureWatchdog;
  private final PollRunner runner;

  /**
   * Construct a new poller which will run on the provided scheduler, and run at the specified
   * frequency.
   *
   * @param scheduler Scheduler to run polling task on
   * @param pollFrequency Time in milliseconds to wait between polling events
   */
  public Poller(SubmitterScheduler scheduler, long pollFrequency) {
    this(scheduler, pollFrequency, -1);
  }

  /**
   * Construct a new poller which will run on the provided scheduler, and run at the specified
   * frequency.
   * <p>
   * This constructor additionally allows you to specify the maximum time in millseconds we should
   * wait for the condition to become true.  At this point if we are still not seeing our expected
   * polling state, then the return future will be canceled.
   *
   * @param scheduler Scheduler to run polling task on
   * @param pollFrequency Time in milliseconds to wait between polling events
   * @param maxWaitTime Maximum time in milliseconds till returned futures should be canceled
   */
  public Poller(SubmitterScheduler scheduler, long pollFrequency, long maxWaitTime) {
    this.scheduler = scheduler;
    if (maxWaitTime > 0 && maxWaitTime != Long.MAX_VALUE) {
      futureWatchdog = new Watchdog(scheduler, maxWaitTime, false);
    } else {
      futureWatchdog = null;
    }
    this.runner = new PollRunner(scheduler, pollFrequency);
  }

  /**
   * Watch suppliers returned condition.  Once Supplier is witnessed in the {@code true} state, the
   * returned future is completed.  Listeners and FutureCallback's executed on the returned future
   * without a specified pool will run on the polling thread, and so should be kept to a minimum.
   *
   * @param p Supplier to provide state for when poll has completed as expected
   * @return Future to complete once boolean state is witnessed as {@code true}
   */
  public ListenableFuture<?> watch(Supplier<Boolean> p) {
    ListenableFuture<?> result = runner.watch(p);
    if (futureWatchdog != null) {
      futureWatchdog.watch(result);
    }
    return result;
  }

  /**
   * Convert a conventional {@link Future} into a {@link ListenableFuture}.  As poller runs it 
   * will check if the provided future has completed.  Once it does complete the returned future 
   * will also complete in the exact same way.  Canceling the returned future will have NO impact 
   * on the provided future (and thus the use with a timeout is not a concern to interrupting the 
   * provided future).  Because this is only checked at poll intervals the returned future's 
   * completion will be delayed by that polling delay.
   * 
   * @param <T> Type of object returned from future
   * @param f Future to monitor for completetion
   * @return ListenableFuture that will provide the result from the source future
   */
  @SuppressWarnings("unchecked")
  public <T> ListenableFuture<T> watch(Future<? extends T> f) {
    if ((futureWatchdog == null || f.isDone()) && f instanceof ListenableFuture) {
      return (ListenableFuture<T>)f;
    }
    ListenableFuture<T> result = runner.watch(f);
    if (futureWatchdog != null) {
      futureWatchdog.watch(result);
    }
    return result;
  }
  
  /**
   * Consumes from a queue, checking for items and providing them to a {@link Consumer} as they 
   * become available.  The poll interval is not the minimum consumption speed, but rather the 
   * resolution / delay that will occur after the queue is seen in an empty state.  If items are 
   * available they will be provided to the {@link Consumer} as fast as possible.  The
   * {@link Consumer} will not be invoked in parallel, so if concurrent consumption is desired it 
   * must execute to a pool.
   * <p>
   * Because queue consumption is likely a long running process, this operation will ignore any 
   * timeout provided to the {@link #Poller(SubmitterScheduler, long, long)} constructor.
   * <p>
   * The returned future will never complete on its own.  It can only be used for stopping the 
   * consumption from the queue by invoking {@link ListenableFuture#cancel(boolean)}.  If 
   * you never want to stop the queue consumption then the returned {@link ListenableFuture} can be 
   * ignored.
   * <p>
   * If any errors occur {@link ExceptionUtils#handleException(Throwable)} will be invoked.
   * 
   * @since 5.37
   * @param queue The queue to poll items from as they are available
   * @param consumer The {@link Consumer} to provide items to as they are available
   * @return A future which can stop consumption through {@code cancel(boolean)}
   */
  public <T> ListenableFuture<?> consumeQueue(Queue<? extends T> queue, 
                                              Consumer<? super T> consumer) {
    return consumeQueue(queue, consumer, ExceptionUtils::handleException);
  }
  
  /**
   * Consumes from a queue, checking for items and providing them to a {@link Consumer} as they 
   * become available.  The poll interval is not the minimum consumption speed, but rather the 
   * resolution / delay that will occur after the queue is seen in an empty state.  If items are 
   * available they will be provided to the {@link Consumer} as fast as possible.  The
   * {@link Consumer} will not be invoked in parallel, so if concurrent consumption is desired it 
   * must execute to a pool.
   * <p>
   * Because queue consumption is likely a long running process, this operation will ignore any 
   * timeout provided to the {@link #Poller(SubmitterScheduler, long, long)} constructor.
   * <p>
   * The returned future may never complete.  It will only complete if an error occurs (and 
   * parameter {@code exceptionHandler} was {@code null}), or if it is canceled.  Invoking 
   * {@link ListenableFuture#cancel(boolean)} on the returned future is how you would stop 
   * providing queue items to the {@link Consumer}.  If an {@code exceptionHandler} is provided, 
   * and you  never want to stop with a {@code cancel(boolean)}, the returned future can be 
   * ignored.
   * 
   * @since 5.37
   * @param queue The queue to poll items from as they are available
   * @param consumer The {@link Consumer} to provide items to as they are available
   * @param exceptionHandler An optional handler for unexpected errors, or {@code null} to stop consumption on error
   * @return A future which can stop consumption through {@code cancel(boolean)}, and will report unhandled errors
   */
  public <T> ListenableFuture<?> consumeQueue(Queue<? extends T> queue, 
                                              Consumer<? super T> consumer, 
                                              ExceptionHandler exceptionHandler) {
    SettableListenableFuture<?> result = new SettableListenableFuture<>(false);
    Supplier<Boolean> readyTest = () -> ! queue.isEmpty() || result.isDone();
    
    runner.watch(readyTest).callback(new FutureCallback<Object>() {
      @Override
      public void handleResult(Object ignored) {
        try {
          ListenableFuture<?> nextReadyLF;
          do {  // loop to reduce risk of StackOverflow
            if (result.isDone()) {
              return; // canceled to indicate stop or completed in error
            }
            
            T item = queue.remove();  // remove before we start next watch
            nextReadyLF = runner.watch(readyTest);
            consumer.accept(item);
          } while (nextReadyLF.isDone());
          
          nextReadyLF.callback(this, scheduler, ListenerOptimizationStrategy.InvokingThreadIfDone);
        } catch (Throwable t) {
          handleFailure(t);
        }
      }

      @Override
      public void handleFailure(Throwable t) {
        if (exceptionHandler == null) {
          result.setFailure(t);
        } else {
          try {
            exceptionHandler.handleException(t);
          } finally {
            runner.watch(readyTest)
                  .callback(this, scheduler, ListenerOptimizationStrategy.InvokingThreadIfDone);
          }
        }
      }
    }, scheduler);
    
    return result;
  }

  /**
   * Class which when run checks across a collection of polls, looking for ones which have 
   * completed.  The runner reschedules itself as long as there is polls to be checked.
   */
  protected static class PollRunner extends ReschedulingOperation {
    private final Collection<Pair<ListenableRunnableFuture<?>, Supplier<Boolean>>> polls =
        new ConcurrentLinkedQueue<>();
    
    public PollRunner(SubmitterScheduler scheduler, long scheduleDelay) {
      super(scheduler, scheduleDelay);
    }

    public ListenableFuture<?> watch(Supplier<Boolean> p) {
      if (p.get()) {
        return ImmediateResultListenableFuture.NULL_RESULT;
      } else {
        ListenableRunnableFuture<?> result =
            new ListenableFutureTask<>(false, DoNothingRunnable.instance(), null, executor);
        polls.add(new Pair<>(result, p));
        signalToRun();
        return result;
      }
    }

    public <T> ListenableFuture<T> watch(Future<? extends T> f) {
      if (f.isDone()) {  // optimized path for already complete futures if possible
        if (f.isCancelled()) {
          SettableListenableFuture<T> slf = new SettableListenableFuture<>();
          slf.cancel(false);
          return slf;
        }
        try {
          return FutureUtils.immediateResultFuture(f.get());
        } catch (ExecutionException e) { // failure in getting result from future, transfer failure
          return FutureUtils.immediateFailureFuture(e.getCause());
        } catch (InterruptedException e) { // should not be possible
          throw new RuntimeException(e);
        }
      } else {
        ListenableRunnableFuture<T> result = new ListenableFutureAdapterTask<T>(f);
        polls.add(new Pair<>(result, f::isDone));
        signalToRun();
        return result;
      }
    }

    @Override
    public void run() {
      Iterator<Pair<ListenableRunnableFuture<?>, Supplier<Boolean>>> it = polls.iterator();
      boolean hasMore = false;
      while (it.hasNext()) {
        Pair<ListenableRunnableFuture<?>, Supplier<Boolean>> p = it.next();
        if (p.getLeft().isDone()) {
          it.remove();  // likely completed/canceled by Watchdog
        } else if (p.getRight().get()) {
          it.remove();
          p.getLeft().run(); // mark as done
        } else {
          hasMore = true;
        }
      }

      if (hasMore) {
        signalToRun();
      }
    }
  }
}
