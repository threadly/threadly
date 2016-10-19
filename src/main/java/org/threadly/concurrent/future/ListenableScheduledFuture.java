package org.threadly.concurrent.future;

import java.util.concurrent.ScheduledFuture;

/**
 * Interface which includes the {@link ScheduledFuture} interface as well as the 
 * {@link ListenableFuture} interface.
 * <p>
 * This is almost identically to {@link java.util.concurrent.ScheduledFuture} except it provides 
 * the additional functionality of the {@link ListenableFuture}.
 * 
 * @since 1.0.0
 * @param <T> The result object type returned by this future
 */
public interface ListenableScheduledFuture<T> extends ScheduledFuture<T>, ListenableFuture<T> {
  // nothing added here
}
