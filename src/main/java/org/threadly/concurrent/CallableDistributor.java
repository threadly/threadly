package org.threadly.concurrent;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.lock.LockFactory;
import org.threadly.concurrent.lock.NativeLockFactory;
import org.threadly.concurrent.lock.VirtualLock;

public class CallableDistributor<K, R> {
  private static final boolean RESULTS_EXPECTED_DEFAULT = true;
  
  private final TaskExecutorDistributor taskDistributor;
  private final VirtualLock callLock;
  private final Map<K, AtomicInteger> waitingCalls;
  private final Map<K, LinkedList<Result<R>>> results;
  
  public CallableDistributor(int expectedParallism, int maxThreadCount) {
    this(new TaskExecutorDistributor(expectedParallism, maxThreadCount));
  }
  
  public CallableDistributor(Executor executor) {
    this(new TaskExecutorDistributor(executor));
  }
  
  public CallableDistributor(TaskExecutorDistributor taskDistributor) {
    this(taskDistributor, new NativeLockFactory());
  }
  
  protected CallableDistributor(TaskExecutorDistributor taskDistributor, 
                                LockFactory lockFactory) {
    if (taskDistributor == null) {
      throw new IllegalArgumentException("Must provide taskDistributor");
    }
    
    this.taskDistributor = taskDistributor;
    callLock = lockFactory.makeLock();
    waitingCalls = new HashMap<K, AtomicInteger>();
    results = new HashMap<K, LinkedList<Result<R>>>();
  }
  
  public void submit(K key, Callable<? extends R> callable) {
    AtomicInteger waitingCount = waitingCalls.get(key);
    if (waitingCount == null) {
      waitingCount = new AtomicInteger();
      waitingCalls.put(key, waitingCount);
    }
    waitingCount.incrementAndGet();
      
    CallableContainer cc = new CallableContainer(key, callable);
    taskDistributor.addTask(key, cc);
  }
  
  // must be locked around callLock
  protected void verifyWaitingForResult(K key) {
    AtomicInteger waitingCount = waitingCalls.get(key);
    if (waitingCount == null || waitingCount.get() == 0) {
      throw new IllegalStateException("No submitted calls currently running for key: " + key);
    }
  }
  
  public Result<R> getNextResult(K key) throws InterruptedException {
    return getNextResult(key, RESULTS_EXPECTED_DEFAULT);
  }
  
  public Result<R> getNextResult(K key, boolean resultsExpected) throws InterruptedException {
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
  
  public List<Result<R>> getAllResults(K key) throws InterruptedException {
    return getAllResults(key, RESULTS_EXPECTED_DEFAULT);
  }
  
  public List<Result<R>> getAllResults(K key, boolean resultsExpected) throws InterruptedException {
    synchronized (callLock) {
      if (resultsExpected) {
        verifyWaitingForResult(key);
      }
      
      List<Result<R>> resultList = results.remove(key);
      while (resultList == null) {
        callLock.await();
        
        resultList = results.get(key);
      }
      
      return resultList;
    }
  }
  
  protected void handleSuccessResult(K key, R result) {
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
  
  protected class CallableContainer implements Runnable {
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
  
  public class Result<R> {
    private final R successResult;
    private final Throwable failureResult;
    
    private Result(R successResult) {
      this.successResult = successResult;
      failureResult = null;
    }
    
    private Result(Throwable failureResult) {
      successResult = null;
      this.failureResult = failureResult;
    }
    
    public R getResult() throws ExecutionException {
      if (failureResult != null) {
        throw new ExecutionException(failureResult);
      }
      
      return successResult;
    }
    
    public Throwable getFailure() {
      return failureResult;
    }
  }
}
