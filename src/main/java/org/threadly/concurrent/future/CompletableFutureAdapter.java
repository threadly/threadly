package org.threadly.concurrent.future;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

/**
 * This class helps in converting between threadly's {@link ListenableFuture} and java's provided 
 * {@link CompletableFuture}.  The threadly project prefers {@link ListenableFuture} as it is able 
 * to be more performant, as well as in some cases provide additional features.  Still, the 
 * concepts are very similar, making it easy for this class to convert between the types.
 * 
 * @since 5.43
 */
public class CompletableFutureAdapter {
  /**
   * Convert from a {@link ListenableFuture} to {@link CompletableFuture}.
   * 
   * @param <T> Type of result provided by the future
   * @param lf The future to adapt into a {@link CompletableFuture}
   * @return A CompletableFuture instance which's state will be connected to the provided future
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <T> CompletableFuture<T> toCompletable(ListenableFuture<? extends T> lf) {
    // no instanceof optimization so that CompletableFuture.getNumberOfDependents() behavior is predictable
    return new AdaptedCompletableFuture<T>(lf);
  }

  /**
   * Convert from a {@link CompletableFuture} to {@link ListenableFuture}.
   * 
   * @param <T> Type of result provided by the future
   * @param cf The future to adapt into a {@link ListenableFuture}
   * @return A ListenableFuture instance which's state will be connected to the provided future
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <T> ListenableFuture<T> toListenable(CompletableFuture<? extends T> cf) {
    if (cf instanceof ListenableFuture) {
      return (ListenableFuture)cf;
    } else if (cf.isDone() && ! cf.isCompletedExceptionally()) {
      try {
        return FutureUtils.immediateResultFuture(cf.get());
      } catch (InterruptedException e) {
        // should not be possible
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        // should not be possible, rather than throw we just adapt
        return FutureUtils.immediateFailureFuture(e.getCause());
      }
    }
    
    return new AdaptedListenableFuture<T>(cf);
    
  }
  
  /**
   * Instance of {@link CompletableFuture} which is dependent on a provided 
   * {@link ListenableFuture} for the state and result.  This class still implements the 
   * {@link ListenableFuture} interface to allow it to be easily converted back if desired.
   *
   * @param <T> Type of result to be provided from the future
   * @since 5.43
   */
  protected static class AdaptedCompletableFuture<T> extends CompletableFuture<T> 
                                                     implements ListenableFuture<T> {
    protected final ListenableFuture<? extends T> lf;
    
    public AdaptedCompletableFuture(ListenableFuture<? extends T> lf) {
      this.lf = lf;
      
      lf.callback(new FutureCallback<T>() {
        @Override
        public void handleResult(T result) {
          complete(result);
        }

        @Override
        public void handleFailure(Throwable t) {
          completeExceptionally(t);
        }
      });
    }

    @Override
    public ListenableFuture<T> listener(Runnable listener, Executor executor,
                                        ListenerOptimizationStrategy optimizeExecution) {
      lf.listener(listener, executor, optimizeExecution);
      
      return this;
    }

    @Override
    public StackTraceElement[] getRunningStackTrace() {
      return lf.getRunningStackTrace();
    }
  }

  /**
   * Instance of {@link ListenableFuture} which is dependent on a provided 
   * {@link CompletableFuture} for the state and result.
   *
   * @param <T> Type of result to be provided from the future
   * @since 5.43
   */
  protected static class AdaptedListenableFuture<T> extends SettableListenableFuture<T> {
    protected final CompletableFuture<? extends T> cf;

    public AdaptedListenableFuture(CompletableFuture<? extends T> cf) {
      this.cf = cf;
      
      cf.whenComplete((result, error) -> {
        if (error != null) {
          setFailure(error);
        } else {
          setResult(result);
        }
      });
    }
  }
}
