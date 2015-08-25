package org.threadly.concurrent;

/**
 * <p>A thread pool for executing tasks with provided futures.  This executor submits 
 * runnables/callables and returns futures for when they will be completed.</p>
 * 
 * @deprecated Please use {@link SubmitterExecutor}
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
@Deprecated
public interface SubmitterExecutorInterface extends SubmitterExecutor {
  // nothing to be removed with this deprecated interface
}
