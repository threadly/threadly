package org.threadly.concurrent;

/**
 * A class to chain multiple runnables to later be run together, within the same thread.
 * 
 * @deprecated Moved to {@link org.threadly.concurrent.wrapper.RunnableChain}
 * 
 * @since 1.0.0
 */
@Deprecated
public class RunnableChain extends org.threadly.concurrent.wrapper.RunnableChain {
  /**
   * Constructs a runnable chain with a provided list of runnables to iterate over.
   * 
   * @param exceptionStopsChain {@code true} for uncaught exception stops the execution of the chain
   * @param runnables Runnables to execute in chain
   */
  public RunnableChain(boolean exceptionStopsChain, Runnable ... runnables) {
    super(exceptionStopsChain, runnables);
  }
  
  /**
   * Constructs a runnable chain with a provided list of runnables to iterate over.
   * 
   * @param exceptionStopsChain {@code true} for uncaught exception stops the execution of the chain
   * @param toRun Iterable collection of runnables to run
   */
  public RunnableChain(boolean exceptionStopsChain, Iterable<? extends Runnable> toRun) {
    super(exceptionStopsChain, toRun);
  }
}
