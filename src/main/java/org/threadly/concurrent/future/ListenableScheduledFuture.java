package org.threadly.concurrent.future;

import java.util.concurrent.ScheduledFuture;

/**
 * <p>Interface which includes the {@link ScheduledFuture} interface as well 
 * as the {@link ListenableFuture} interface.</p>
 * 
 * <p>This is almost identically to java.util.concurrent.ScheduledFuture except it 
 * provides the additional functionality of the {@link ListenableFuture}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 * @param <T> type of result for future
 */
public interface ListenableScheduledFuture<T> extends ScheduledFuture<T>, ListenableFuture<T> {
  // nothing added here
}
