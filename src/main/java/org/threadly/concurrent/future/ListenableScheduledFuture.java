package org.threadly.concurrent.future;

import java.util.concurrent.ScheduledFuture;

/**
 * Interface which includes the {@link ScheduledFuture} interface as well 
 * as the {@link ListenableFuture} interface.
 * 
 * This is almost identically to java.util.concurrent.ScheduledFuture excepto it 
 * provides the additional functionality of the {@link ListenableFuture}.
 * 
 * @author jent - Mike Jensen
 * @param <T> type of result for future
 */
public interface ListenableScheduledFuture<T> extends ScheduledFuture<T>, ListenableFuture<T> {
  
}
