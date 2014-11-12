package org.threadly.concurrent.future;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <p>This is a slight modification to the behavior of the parent class 
 * {@link SettableListenableFuture}.  In this implementation, after the first {@link #get()} call 
 * the stored result will be cleared (thus allowing it to be available for garbage collection).</p>
 * 
 * <p>Because the result is cleared after the first {@link #get()} call, {@link #get()} can ONLY 
 * BE INVOKED ONCE.  Additional invocations will result in a {@link IllegalStateException}.  This 
 * implementation only makes sense in specific uses cases (for example when chaining 
 * {@link SettableListenableFuture}'s together).</p>
 * 
 * @author jent - Mike Jensen
 * @since 3.3.0
 * @param <T> The result object type returned by this future
 */
public class ClearOnGetSettableListenableFuture<T> extends SettableListenableFuture<T> {
  @Override
  public T get() throws InterruptedException, ExecutionException {
    try {
      T result = super.get();
    
      return result;
    } finally {
      clearResult();
    }
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, 
                                                   ExecutionException,
                                                   TimeoutException {
    boolean timeoutThrown = false;
    try {
      T result = super.get(timeout, unit);
      
      return result;
    } catch (TimeoutException e) {
      timeoutThrown = true;
      throw e;
    } finally {
      // we can't clear if we timed out, because the result was never set
      if (! timeoutThrown) {
        clearResult();
      }
    }
  }
}
