package org.threadly.concurrent.wrapper;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.future.ListenableFuture;

/**
 * Threadly implementation of {@link CompletionService}.  This will take in any {@link Executor} 
 * and provide back a threadly {@link ListenableFuture}.
 *
 * @param <T> Type of result from tasks
 */
public class ThreadlyCompletionService<T> implements CompletionService<T> {
  protected final SubmitterExecutor executor;
  private final LinkedBlockingQueue<ListenableFuture<T>> completedQueue;
  
  /**
   * Construct a new {@link ThreadlyCompletionService} ready to accept tasks.
   * 
   * @param executor Executor to distributor submitted tasks to
   */
  public ThreadlyCompletionService(Executor executor) {
    this.executor = SubmitterExecutorAdapter.adaptExecutor(executor);
    this.completedQueue = new LinkedBlockingQueue<>();
  }

  @Override
  public ListenableFuture<T> submit(Runnable runnable, T result) {
    return submit(RunnableCallableAdapter.adapt(runnable, result));
  }

  @Override
  public ListenableFuture<T> submit(Callable<T> task) {
    ListenableFuture<T> lf = executor.submit(task);
    lf.listener(() -> completedQueue.add(lf));
    return lf;
  }

  @Override
  public ListenableFuture<T> poll() {
    return completedQueue.poll();
  }

  @Override
  public ListenableFuture<T> poll(long timeout, TimeUnit unit) throws InterruptedException {
    return completedQueue.poll(timeout, unit);
  }

  @Override
  public ListenableFuture<T> take() throws InterruptedException {
    return completedQueue.take();
  }
}
