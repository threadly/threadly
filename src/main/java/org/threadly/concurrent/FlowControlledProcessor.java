package org.threadly.concurrent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import org.threadly.concurrent.future.FutureCallback;
import org.threadly.concurrent.future.FutureUtils;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.SettableListenableFuture;
import org.threadly.util.ArgumentVerifier;

/**
 * Abstract implementation which will do async processing, but only submit a limited number of 
 * tasks processing concurrently.  This is different from something like an ExecutorLimiter 
 * because it not only limits execution, but pulls for new tasks when ready.  This makes it easier 
 * to limit how much is held in heap.  Otherwise while pools may limit execution to prevent 
 * over utilizing resources, this will help prevent task submission to ensure that the running 
 * processor does not over-consume its heap.
 * <p>
 * This object is one time use.  Once {@link #hasNext()} returns {@code false} it should be 
 * considered done forever.  If more work needs to be processed then this object needs to be 
 * reconstructed with a new instance.
 * 
 * @since 5.37
 * @param <T> The type of result produced / accepted
 */
public abstract class FlowControlledProcessor<T> {
  protected final int maxRunningTasks;
  private final SettableListenableFuture<Void> completeFuture;
  private final ProcessingMonitor futureMonitor;
  private boolean started = false;
  
  /**
   * Construct a new processor.  You must invoke {@link #start()} once constructed to start 
   * processing.
   * 
   * @param maxRunningTasks Maximum number of concurrent running tasks
   * @param provideResultsInOrder If {@code true} completed results will be provided in the order they are submitted
   */
  public FlowControlledProcessor(int maxRunningTasks, boolean provideResultsInOrder) {
    ArgumentVerifier.assertGreaterThanZero(maxRunningTasks, "maxRunningTasks");
    
    this.maxRunningTasks = maxRunningTasks;
    this.completeFuture = new SettableListenableFuture<>(false);
    this.futureMonitor = 
        provideResultsInOrder ? new InOrderProcessingMonitor() : new ProcessingMonitor();
  }
  
  /**
   * Start processing tasks.  The returned future wont complete until task execution has completed 
   * or unless {@link #handleFailure(Throwable)} returns {@code false} indicating an error is not 
   * able to be handled.  In the case of an unhandled error the returned future will finish with 
   * the error condition.
   *   
   * @return Future to indicate state of completion
   */
  public ListenableFuture<?> start() {
    synchronized (this) {
      if (started) {
        throw new IllegalStateException("Already started");
      }
      started = true;
    }
    
    futureMonitor.start();
    
    return completeFuture;
  }
  
  /**
   * Checked before {@link #next()} is invoked to see if there is more work to do.  If this 
   * returns {@code true} then {@link #next()} will be invoked.  It should be expected that 
   * this state may be checked multiple times before {@link #next()} is invoked.  Once this 
   * returns {@code false} it should remain as completed forever.
   * 
   * @return {@code true} to indicate there is more to process, {@code false} to indicate completion
   */
  protected abstract boolean hasNext();
  
  /**
   * Invoked when ready to process the next item.  This will only be invoked after 
   * {@link #hasNext()} returns {@code true}.  This wont be invoked concurrently, so blocking on 
   * this function can result in slowing down task execution.  Instead this should immediately 
   * return a {@link ListenableFuture} which will complete when processing is done.  Once the 
   * returned future completes the result will be provided to {@link #handleResult(Object)}.  If 
   * the returned future completes in error then instead {@link #handleFailure(Throwable)} will be 
   * invoked with the failure.
   * 
   * @return A non-null {@link ListenableFuture} for when the processing completes
   */
  protected abstract ListenableFuture<? extends T> next();
  
  /**
   * Invoked when the processing is completed.  If this class is constructed to allow out of order 
   * completion then this may be invoked concurrently, if synchronization is necessary it must be 
   * done in the implementing class.
   * 
   * @param result Result provided from future returned from {@link #next()}
   */
  protected abstract void handleResult(T result);
  
  /**
   * Invoked when an error occurred, either from a returned future from {@link #next()} or 
   * when one of the implemented functions throws an unexpected error.  This function should return 
   * {@code true} if the error is expected or able to be handled, or {@code false} if it wants the 
   * submission of new tasks to stop and instead communication the error to the future returned 
   * from {@link #start()}.
   * 
   * @param t The failure thrown
   * @return {@code true} to indicate error is handled and processing should continue
   */
  protected abstract boolean handleFailure(Throwable t);
  
  /**
   * Class for monitoring tasks submitted and when they can start new ones.  This is where the end 
   * of events will eventually be handled, and complete the final {@link #completeFuture}.
   * 
   * @since 5.37
   */
  protected class ProcessingMonitor implements FutureCallback<T> {
    private int currentRunningTasks = 0;
    
    /**
     * Starts running the initial tasks, once initial calls to {@link #next()} are done this will 
     * return.
     */
    public void start() {
      List<ListenableFuture<? extends T>> futures = new ArrayList<>(maxRunningTasks); 
      while (currentRunningTasks < maxRunningTasks && ! completeFuture.isDone() && hasNext()) {
        currentRunningTasks++;
        try {
          futures.add(next());
        } catch (Exception e) {
          handleFailure(e);
        }
      }

      if (currentRunningTasks == 0) { // no tasks it seems
        completeFuture.setResult(null);
      } else {
        // add callback after to avoid any delays in handling results and any concurrency issues
        for (ListenableFuture<? extends T> lf : futures) {
          lf.callback(this);
        }
      }
    }
    
    // defined so it can be overriden
    protected ListenableFuture<? extends T> next() {
      return FlowControlledProcessor.this.next();
    }
    
    @Override
    public void handleResult(T result) {
      try {
        FlowControlledProcessor.this.handleResult(result);
        readyForNext();
      } catch (Throwable t) {
        handleFailure(t);
      }
    }

    @Override
    public void handleFailure(Throwable t) {
      if (FlowControlledProcessor.this.handleFailure(t)) {
        readyForNext();
      } else {
        completeFuture.setFailure(t);
      }
    }
    
    /**
     * Invoked once processing has completed and we are ready to start the next one.  This may be 
     * called concurrently.  Once there is no remaining work and all processing has completed this 
     * will set the {@link completeFuture} with a {@code null} result.
     */
    protected void readyForNext() {
      ListenableFuture<? extends T> nextFuture; // return to avoid `null` conditions with nextFuture
      synchronized (FlowControlledProcessor.this) {
        try {
          if (completeFuture.isDone() || ! hasNext()) {
            currentRunningTasks--;
            if (currentRunningTasks == 0) {
              completeFuture.setResult(null);
            }
            return;
          } else {
            nextFuture = next();
          }
        } catch (Throwable t) {
          handleFailure(t);
          return;
        }
      }
      
      nextFuture.callback(this);  // add callback outside of lock
    }
  }
  
  /**
   * Extending implementation where results (failed or not) will be provided in the order they were 
   * submitted for execution (rather than the order they complete).
   * 
   * @since 5.37
   */
  protected class InOrderProcessingMonitor extends ProcessingMonitor {
    protected final ReschedulingOperation futureConsumer = 
        // ReschedulingOperation to provide non-concurrent providing results from completed futures
        new ReschedulingOperation(SameThreadSubmitterExecutor.instance()) {
          @Override
          protected void run() {
            ListenableFuture<? extends T> lf;
            while ((lf = nextReadyFuture()) != null) {
              try {
                try {
                  FlowControlledProcessor.this.handleResult(lf.get());
                } catch (ExecutionException e) {
                  handleInOrderFailure(e.getCause());
                } catch (CancellationException e) {
                  handleInOrderFailure(e);
                } catch (InterruptedException e) {
                  // not possible
                  throw new RuntimeException(e);
                }
              } catch (Throwable t) {
                handleInOrderFailure(t);
              }
            }
          }
          
          protected ListenableFuture<? extends T> nextReadyFuture() {
            synchronized (futureQueue) {
              if (! futureQueue.isEmpty() && futureQueue.peek().isDone()) {
                return futureQueue.remove();
              }
            }
            return null;
          }
          
          protected void handleInOrderFailure(Throwable t) {
            if (! FlowControlledProcessor.this.handleFailure(t)) {
              completeFuture.setFailure(t);
            }
          }
        };
    protected Queue<ListenableFuture<? extends T>> futureQueue = new ArrayDeque<>(maxRunningTasks);
    
    @Override
    public void handleResult(T result) {
      try {
        futureConsumer.signalToRun();
      } finally {
        readyForNext();
      }
    }

    @Override
    public void handleFailure(Throwable t) {
      try {
        futureConsumer.signalToRun();
      } finally {
        readyForNext();
      }
    }
    
    @Override
    protected ListenableFuture<? extends T> next() {
      ListenableFuture<? extends T> result;
      try {
        result = FlowControlledProcessor.this.next();
      } catch (Throwable t) {
        result = FutureUtils.immediateFailureFuture(t);
      }
      synchronized (futureQueue) {
        futureQueue.add(result);
      }
      return result;
    } 
  }
}
