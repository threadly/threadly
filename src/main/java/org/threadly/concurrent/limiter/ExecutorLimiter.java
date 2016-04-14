package org.threadly.concurrent.limiter;

import java.util.concurrent.Executor;

import org.threadly.concurrent.SubmitterExecutorInterface;
import org.threadly.concurrent.wrapper.traceability.ThreadRenamingExecutorWrapper;
import org.threadly.util.StringUtils;

/**
 * <p>This class is designed to limit how much parallel execution happens on a provided 
 * {@link Executor}.  This allows the user to have one thread pool for all their code, and if they 
 * want certain sections to have less levels of parallelism (possibly because those those sections 
 * would completely consume the global pool), they can wrap the executor in this class.</p>
 * 
 * <p>Thus providing you better control on the absolute thread count and how much parallelism can 
 * occur in different sections of the program.</p>
 * 
 * <p>This is an alternative from having to create multiple thread pools.  By using this you also 
 * are able to accomplish more efficiently thread use than multiple thread pools would.</p>
 * 
 * @deprecated Moved to {@link org.threadly.concurrent.wrapper.limiter.ExecutorLimiter}
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
@Deprecated
public class ExecutorLimiter extends org.threadly.concurrent.wrapper.limiter.ExecutorLimiter
                             implements SubmitterExecutorInterface {
  /**
   * Construct a new execution limiter that implements the {@link Executor} interface.
   * 
   * @param executor {@link Executor} to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   */
  public ExecutorLimiter(Executor executor, int maxConcurrency) {
    this(executor, maxConcurrency, null);
  }
  
  /**
   * Construct a new execution limiter that implements the {@link Executor} interface.
   * 
   * @deprecated Rename threads using {@link ThreadRenamingExecutorWrapper} to rename executions from this limiter
   * 
   * @param executor {@link Executor} to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   * @param subPoolName name to describe threads while tasks running in pool ({@code null} to not change thread names)
   */
  @Deprecated
  public ExecutorLimiter(Executor executor, int maxConcurrency, String subPoolName) {
    super(executor == null ? null : 
            StringUtils.isNullOrEmpty(subPoolName) ? 
              executor : new ThreadRenamingExecutorWrapper(executor, subPoolName, false), 
          maxConcurrency);
  }
}
