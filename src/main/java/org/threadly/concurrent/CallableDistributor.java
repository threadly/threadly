package org.threadly.concurrent;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.lock.NativeLockFactory;
import org.threadly.concurrent.lock.StripedLock;
import org.threadly.concurrent.lock.VirtualLock;

/**
 * This abstraction is designed to make submitting callable tasks with 
 * results easier.  Tasks are submitted and results for those tasks can be 
 * captured when ready using .getNextResult() or .getAllResults().  
 * If multiple callables are submitted with the same key, they are guaranteed to 
 * never run in parallel.
 * 
 * @author jent - Mike Jensen
 *
 * @param <K> Key for distributing callables and getting results
 * @param <R> Type of result that is returned
 */
public class CallableDistributor<K, R> {
  private static final boolean RESULTS_EXPECTED_DEFAULT = true;
  
  private final TaskExecutorDistributor taskDistributor;
  private final StripedLock sLock;
  private final ConcurrentHashMap<K, AtomicInteger> waitingCalls;
  private final Map<K, LinkedList<Result<R>>> results;  // locked around sLock for key
  
  /**
   * Constructs a new CallableDistributor with giving parameters to 
   * help guide thread pool construction.
   * 
   * @param expectedParallism expected qty of keys to be used in parallel
   * @param maxThreadCount maximum threads to be used
   */
  public CallableDistributor(int expectedParallism, int maxThreadCount) {
    this(new TaskExecutorDistributor(expectedParallism, maxThreadCount));
  }
  
  /**
   * Constructs a new CallableDistributor with a provided Executor.  
   * Executor should have enough available threads to be able to service the 
   * expected key quantity in parallel.
   * 
   * @param executor Executor for new threads to be executed on
   */
  public CallableDistributor(Executor executor) {
    this(new TaskExecutorDistributor(executor));
  }
  
  /**
   * Constructs a new CallableDistributor with a provided Executor.  
   * Executor should have enough available threads to be able to service the 
   * expected key quantity in parallel.
   * 
   * @param expectedParallism  level of expected qty of threads adding callables in parallel
   * @param executor Executor for new threads to be executed on
   */
  public CallableDistributor(int expectedParallism, Executor executor) {
    this(new TaskExecutorDistributor(expectedParallism, executor));
  }

  
  /**
   * Constructs a new CallableDistributor with a provided taskDistributor.
   * 
   * @param taskDistributor TaskDistributor used to execute callables
   */
  public CallableDistributor(TaskExecutorDistributor taskDistributor) {
    this(taskDistributor, new StripedLock(taskDistributor.sLock.getExpectedConcurrencyLevel(), 
                                          new NativeLockFactory()));
  }
  
  /**
   * Constructs for unit testing.
   */
  protected CallableDistributor(TaskExecutorDistributor taskDistributor, 
                                StripedLock sLock) {
    if (taskDistributor == null) {
      throw new IllegalArgumentException("Must provide taskDistributor");
    }
    
    this.taskDistributor = taskDistributor;
    this.sLock = sLock;
    waitingCalls = new ConcurrentHashMap<K, AtomicInteger>();
    results = new HashMap<K, LinkedList<Result<R>>>();
  }
  
  /**
   * Submits a new callable to be executed with a given key to determine 
   * thread to be run on, and how to get results for callable.
   * 
   * @param key key for callable thread choice and to get result
   * @param callable to be executed
   */
  public void submit(K key, Callable<? extends R> callable) {
    if (key == null) {
      throw new IllegalArgumentException("key can not be null");
    } else if (callable == null) {
      throw new IllegalArgumentException("callable can not be null");
    }
    
    AtomicInteger waitingCount = waitingCalls.get(key);
    if (waitingCount == null) {
      waitingCount = new AtomicInteger();
      AtomicInteger putResult = waitingCalls.putIfAbsent(key, waitingCount);
      if (putResult != null) {
        waitingCount = putResult;
      }
    }
    waitingCount.incrementAndGet();
      
    CallableContainer cc = new CallableContainer(key, callable);
    taskDistributor.addTask(key, cc);
  }
  
  /**
   * Call to check if results are, or will be ready for a given key.
   * 
   * @param key key for submitted callables
   * @return true if results are either ready, or currently processing for key
   */
  public boolean waitingResults(K key) {
    AtomicInteger waitingCount = waitingCalls.get(key);
    
    if (waitingCount != null && waitingCount.get() > 0) {
      return true;
    } else {
      VirtualLock callLock = sLock.getLock(key);
      synchronized (callLock) {
        return results.containsKey(key);
      }
    }
  }
  
  protected void verifyWaitingForResult(K key) {
    if (! waitingResults(key)) {
      throw new IllegalStateException("No submitted calls currently running for key: " + key);
    }
  }
  
  /**
   * Call to get the next result for a given submission key.  If 
   * there are no callables currently running, and no results 
   * waiting, this will throw an IllegalStateException.
   * 
   * @param key used when submitted that results will be returned for
   * @return result from execution
   * @throws InterruptedException exception if thread was interrupted while blocking
   */
  public Result<R> getNextResult(K key) throws InterruptedException {
    return getNextResult(key, RESULTS_EXPECTED_DEFAULT);
  }
  
  
  /**
   * Call to get the next result for a given submission key.  This 
   * call has the potential to block before a callable has been submitted.
   * 
   * This has the risk of blocking forever if a result has already been 
   * consumed or the callable is never submitted.  But could be useful if the 
   * thread that is polling for results is different from the one submitting.
   * 
   * @param key used when submitted that results will be returned for
   * @param resultsExpected if callable was guaranteed to be already submitted
   * @return result from execution
   * @throws InterruptedException exception if thread was interrupted while blocking
   */
  public Result<R> getNextResult(K key, boolean resultsExpected) throws InterruptedException {
    if (key == null) {
      throw new IllegalArgumentException("key can not be null");
    }
    
    VirtualLock callLock = sLock.getLock(key);
    synchronized (callLock) {
      if (resultsExpected) {
        verifyWaitingForResult(key);
      }
      
      LinkedList<Result<R>> resultList = results.get(key);
      while (resultList == null) {
        callLock.await();
        
        resultList = results.get(key);
      }
      
      Result<R> result = resultList.removeFirst();
      if (resultList.isEmpty()) {
        results.remove(key);
      }
      
      return result;
    }
  }
  
  /**
   * Call to return all results currently available for a given submission 
   * key.  If no results are available, but a callable is running that will 
   * produce one, this call will block till results are ready.  If 
   * there are no callables currently running, and no results 
   * waiting, this will throw an IllegalStateException.
   * 
   * @param key used when submitted that results will be returned for
   * @return a list of all results ready for the given key
   * @throws InterruptedException exception if thread was interrupted while blocking
   */
  public List<Result<R>> getAllResults(K key) throws InterruptedException {
    return getAllResults(key, RESULTS_EXPECTED_DEFAULT);
  }
  
  /**
   * Call to return all results currently available for a given submission 
   * key.  If no results are available, but a callable is running that will 
   * produce one, this call will block till results are ready.
   * 
   * This has the risk of blocking forever if a result has already been 
   * consumed or the callable is never submitted.  But could be useful if the 
   * thread that is polling for results is different from the one submitting.
   * 
   * @param key used when submitted that results will be returned for
   * @param resultsExpected if callable was guaranteed to be already submitted
   * @return a list of all results ready for the given key
   * @throws InterruptedException exception if thread was interrupted while blocking
   */
  public List<Result<R>> getAllResults(K key, boolean resultsExpected) throws InterruptedException {
    if (key == null) {
      throw new IllegalArgumentException("key can not be null");
    }

    VirtualLock callLock = sLock.getLock(key);
    synchronized (callLock) {
      if (resultsExpected) {
        verifyWaitingForResult(key);
      }
      
      List<Result<R>> resultList = results.remove(key);
      while (resultList == null) {
        callLock.await();
        
        resultList = results.remove(key);
      }
      
      return resultList;
    }
  }
  
  protected void handleSuccessResult(K key, R result) {
    VirtualLock callLock = sLock.getLock(key);
    synchronized (callLock) {
      AtomicInteger waitingCount = waitingCalls.get(key);
      if (waitingCount == null || waitingCount.get() < 1) {
        throw new IllegalStateException("Not waiting for result?");
      }
      
      LinkedList<Result<R>> resultList = results.get(key);
      if (resultList == null) {
        resultList = new LinkedList<Result<R>>();
        results.put(key, resultList);
      }
      resultList.add(new Result<R>(result));

      waitingCount.decrementAndGet();
      callLock.signalAll();
    }
  }
  
  protected void handleFailureResult(K key, Throwable t) {
    VirtualLock callLock = sLock.getLock(key);
    synchronized (callLock) {
      AtomicInteger waitingCount = waitingCalls.get(key);
      if (waitingCount == null || waitingCount.get() < 1) {
        throw new IllegalStateException("Not waiting for result?");
      }
      
      LinkedList<Result<R>> resultList = results.get(key);
      if (resultList == null) {
        resultList = new LinkedList<Result<R>>();
        results.put(key, resultList);
      }
      resultList.add(new Result<R>(t));

      waitingCount.decrementAndGet();
      callLock.signalAll();
    }
  }
  
  /**
   * Container for a callable which will record the result after completed.
   * 
   * @author jent - Mike Jensen
   */
  protected class CallableContainer extends VirtualRunnable {
    private final K key;
    private final Callable<? extends R> callable;
    
    public CallableContainer(K key, Callable<? extends R> callable) {
      this.key = key;
      this.callable = callable;
    }
    
    @Override
    public void run() {
      try {
        R result = callable.call();
        handleSuccessResult(key, result);
      } catch (Exception e) {
        handleFailureResult(key, e);
      }
    }
  }
  
  /**
   * Result from a callable which can be either a computed 
   * result, or an exception due to failure.
   * 
   * @author jent - Mike Jensen
   *
   * @param <R> result type
   */
  public static class Result<R> {
    private final R successResult;
    private final Throwable failureResult;
    
    protected Result(R successResult) {
      this.successResult = successResult;
      failureResult = null;
    }
    
    protected Result(Throwable failureResult) {
      successResult = null;
      this.failureResult = failureResult;
    }
    
    /**
     * Will return the result from the callable, or throw an ExecutionException 
     * if a failure occurred during execution.
     * 
     * @return computed result
     * @throws ExecutionException Exception to indicate failure occurred during run
     */
    public R get() throws ExecutionException {
      if (failureResult != null) {
        throw new ExecutionException(failureResult);
      }
      
      return successResult;
    }
    
    /**
     * Will retrieve the stored result, or null if 
     * no result was provided but Exception was thrown.
     * 
     * @return result from execution, null if failure
     */
    public R getResult() {
      return successResult;
    }
    
    
    /**
     * Will provide the throwable that may have been thrown during 
     * execution, or null if none was thrown.
     * 
     * @return throwable that was thrown during execution, or null if none was thrown
     */
    public Throwable getFailure() {
      return failureResult;
    }
  }
}
