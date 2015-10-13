package org.threadly.concurrent.future;

/**
 * <p>Extending interface from {@link ListenableFuture}.  This offers nothing unique but does 
 * allow for less typing.  Please see {@link ListenableFuture} documentation.</p>
 * 
 * @author jent - Mike Jensen
 * @param <T> The result object type returned by this future
 * @since 4.4.0
 */
public interface Promise<T> extends ListenableFuture<T> {
  // nothing added here
}
