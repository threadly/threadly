package org.threadly.concurrent;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.threadly.concurrent.lock.LockFactory;
import org.threadly.concurrent.lock.NativeLockFactory;
import org.threadly.concurrent.lock.VirtualLock;

public class CallableDistributor<K, R> {
  private static final boolean RESULTS_EXPECTED_DEFAULT = true;
  
  private final TaskExecutorDistributor taskDistributor;
  private final VirtualLock callLock;
  private final Map<K, Integer> waitingCalls;
  private final Map<K, LinkedList<? extends R>> results;
  
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
    waitingCalls = new HashMap<K, Integer>();
    results = new HashMap<K, LinkedList<? extends R>>();
  }
  
  public void submit(K key, Callable<? extends R> callable) {
    synchronized (callLock) {
      Integer waitingCount = waitingCalls.get(key);
      if (waitingCount == null) {
        waitingCount = 0;
        waitingCalls.put(key, waitingCount);
      }
      waitingCount++;
      
      CallableContainer cc = new CallableContainer(key, callable);
      taskDistributor.addTask(key, cc);
    }
  }
  
  // must be locked around callLock
  protected void verifyWaitingForResult(K key) {
      Integer waitingCount = waitingCalls.get(key);
      if (waitingCount == null || waitingCount == 0) {
        throw new IllegalStateException("No submitted calls currently running for key: " + key);
      }
  }
  
  public R getNextResult(K key) {
    return getNextResult(key, RESULTS_EXPECTED_DEFAULT);
  }
  
  public R getNextResult(K key, boolean resultsExpected) {
    synchronized (callLock) {
      if (resultsExpected) {
        verifyWaitingForResult(key);
      }
      
      // TODO 
      throw new UnsupportedOperationException();
    }
  }
  
  public List<? extends R> getAllResults(K key) {
    return getAllResults(key, RESULTS_EXPECTED_DEFAULT);
  }
  
  public List<? extends R> getAllResults(K key, boolean resultsExpected) {
    synchronized (callLock) {
      if (resultsExpected) {
        verifyWaitingForResult(key);
      }
      
      // TODO 
      throw new UnsupportedOperationException();
    }
  }
  
  protected void handleSuccessResult(K key, R result) {
    synchronized (callLock) {
      // TODO
      
      callLock.signalAll();
    }
  }
  
  protected void handleFailureResult(K key, Throwable t) {
    synchronized (callLock) {
      // TODO
      
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
}
