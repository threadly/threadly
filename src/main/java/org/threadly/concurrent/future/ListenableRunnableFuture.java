package org.threadly.concurrent.future;

import java.util.concurrent.RunnableFuture;

/**
 * This is a {@link ListenableFuture} which can be executed.  Allowing you to construct the future 
 * with the interior work, submit it to an {@link java.util.concurrent.Executor}, and then return 
 * this future.
 * <p>
 * This is similar to {@link java.util.concurrent.RunnableFuture} except that it provides the 
 * additional functionality from the {@link ListenableFuture} interface.
 * 
 * @since 1.0.0
 * @param <T> The result object type returned by this future
 */
public interface ListenableRunnableFuture<T> extends ListenableFuture<T>, RunnableFuture<T> {
  // nothing added here
}
