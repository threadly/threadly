package org.threadly.concurrent.wrapper;

import java.util.Collections;
import java.util.Iterator;

import org.threadly.util.ArrayIterator;
import org.threadly.util.ExceptionUtils;

/**
 * A class to chain multiple runnables to later be run together, within the same thread.  When 
 * constructed with {@link #RunnableChain(boolean, Iterable)} the iterator is not requested until 
 * execution is starting.  This can allow you to delay knowing which tasks to execute until 
 * execution is starting.
 * 
 * @since 5.40 (existed since 1.0.0 under package org.threadly.concurrent)
 */
public class RunnableChain implements Runnable {
  protected final boolean exceptionStopsChain;
  private final Iterable<? extends Runnable> toRun;

  /**
   * Constructs a runnable chain with a provided array of runnables to iterate over.
   * 
   * @param exceptionStopsChain {@code true} to allow uncaught exceptions to propagate and stop chain execution
   * @param runnables Runnables to execute in chain
   */
  public RunnableChain(boolean exceptionStopsChain, Runnable ... runnables) {
    this(exceptionStopsChain, ArrayIterator.makeIterable(runnables));
  }
  
  /**
   * Constructs a runnable chain with an {@link Iterable}.  The provided {@link Iterable} wont be 
   * requested for {@link Iterable#iterator()} until execution is about to start.  This can allow 
   * a lazy decision for what runnables should be executed.
   * 
   * @param exceptionStopsChain {@code true} to allow uncaught exceptions to propagate and stop chain execution
   * @param toRun Iterable collection of runnables to run
   */
  public RunnableChain(boolean exceptionStopsChain, Iterable<? extends Runnable> toRun) {
    if (toRun == null) {
      toRun = Collections.emptyList();
    }
    
    this.exceptionStopsChain = exceptionStopsChain;
    this.toRun = toRun;
  }

  @Override
  public void run() {
    if (exceptionStopsChain) {
      runExceptionsCascade();
    } else {
      runIsolated();
    }
  }
  
  /**
   * Iterates through the toRun list, executing along the way.  If any exceptions are thrown, they 
   * will be propagated out of this call.
   */
  protected void runExceptionsCascade() {
    Iterator<? extends Runnable> it = toRun.iterator();
    while (it.hasNext()) {
      it.next().run();
    }
  }
  
  /**
   * Iterates through the toRun list, executing along the way.  If any exceptions are thrown, they 
   * will be handled to {@link ExceptionUtils} and will not stop future executions.
   */
  protected void runIsolated() {
    Iterator<? extends Runnable> it = toRun.iterator();
    while (it.hasNext()) {
      try {
        it.next().run();
      } catch (Throwable t) {
        ExceptionUtils.handleException(t);
      }
    }
  }
}
