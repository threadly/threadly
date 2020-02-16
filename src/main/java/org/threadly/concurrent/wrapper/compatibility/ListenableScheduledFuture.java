package org.threadly.concurrent.wrapper.compatibility;

import java.util.concurrent.ScheduledFuture;

import org.threadly.concurrent.future.ListenableFuture;

/**
 * Interface which includes the {@link ScheduledFuture} interface as well as the 
 * {@link ListenableFuture} interface.
 * <p>
 * This is almost identically to {@link java.util.concurrent.ScheduledFuture} except it provides 
 * the additional functionality of the {@link ListenableFuture}.
 * 
 * @since 5.22 (since 1.0.0 under org.threadly.concurrent.future package)
 * @param <T> The result object type returned by this future
 */
@SuppressWarnings("deprecation")
public interface ListenableScheduledFuture<T> extends ScheduledFuture<T>, ListenableFuture<T> {
  // nothing added here
}
