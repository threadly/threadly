package org.threadly.concurrent.future;

import java.util.concurrent.RunnableFuture;

/**
 * This is a {@link ListenableFuture} which can be executed.  Allowing you to construct the future with 
 * the interior work, submit it to an executor, and then return this future.
 * 
 * This is similar to java.util.concurrent.RunnableFuture except that it provides the additional 
 * functionality from the {@link ListenableFuture} interface.
 * 
 * @author jent - Mike Jensen
 * @param <T> type of future implementation
 */
public interface ListenableRunnableFuture<T> extends ListenableFuture<T>, 
                                                     RunnableFuture<T> {
  // nothing added here
}
