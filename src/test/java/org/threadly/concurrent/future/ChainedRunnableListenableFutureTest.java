package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.TestCallable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class ChainedRunnableListenableFutureTest {
  @Test
  public void getCallableResultTest() throws InterruptedException, ExecutionException {
    final Object result = new Object();
    RunnableFuture<Object> future;
    future = new ChainedRunnableListenableFuture<Object>(new Thread(), SameThreadSubmitterExecutor.instance(), 
                                                         new Callable<Object>() {
      @Override
      public Object call() {
        return result;
      }
    });
    
    future.run();
    
    assertTrue(future.get() == result);
  }
  
  @Test
  public void getCallableFailureTest() throws InterruptedException {
    final RuntimeException failure = new RuntimeException();
    RunnableFuture<Void> future;
    future = new ChainedRunnableListenableFuture<Void>(new SettableListenableFuture<Void>(), 
                                                       SameThreadSubmitterExecutor.instance(), 
                                                       new Callable<Void>() {
      @Override
      public Void call() {
        throw failure;
      }
    });
    
    future.run();
    
    try {
      future.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() ==  failure);
    }
  }
  
  @Test
  public void getResultOnExecutorTest() throws InterruptedException, ExecutionException, TimeoutException {
    final AtomicReference<Thread> executeThread = new AtomicReference<Thread>(null);
    final AtomicInteger callCount = new AtomicInteger(0);
    RunnableFuture<Void> future = new ChainedRunnableListenableFuture<Void>(new Executor() {
      @Override
      public void execute(Runnable command) {
        new Thread(command).start();
      }
    }, new Callable<Void>() {
      @Override
      public Void call() {
        executeThread.set(Thread.currentThread());
        callCount.incrementAndGet();
        return null;
      }
    });
    
    future.run();
    future.get(10, TimeUnit.SECONDS); // block till done
    
    assertEquals(1, callCount.get());
    assertTrue(executeThread.get() != Thread.currentThread());
  }
  
  @Test
  public void getResultOnInitialThreadTest() throws InterruptedException, ExecutionException, TimeoutException {
    final AtomicReference<Thread> executeThread = new AtomicReference<Thread>(null);
    final AtomicInteger callCount = new AtomicInteger(0);
    RunnableFuture<Void> future = new ChainedRunnableListenableFuture<Void>(new Executor() {
      @Override
      public void execute(Runnable command) {
        throw new UnsupportedOperationException();
      }
    }, new Callable<Void>() {
      @Override
      public Void call() {
        executeThread.set(Thread.currentThread());
        callCount.incrementAndGet();
        return null;
      }
    });
    
    Thread futureRunThread = new Thread(future);
    futureRunThread.start();
    future.get(10, TimeUnit.SECONDS); // block till done
    
    assertEquals(1, callCount.get());
    assertTrue(executeThread.get() == futureRunThread);
  }
  
  @Test (expected = IllegalStateException.class)
  public void runTwiceFail() {
    RunnableFuture<Object> future;
    future = new ChainedRunnableListenableFuture<Object>(SameThreadSubmitterExecutor.instance(), 
                                                         new TestCallable());
    
    future.run();
    future.run();
  }
  
  @Test
  public void isDoneTest() {
    RunnableFuture<Object> future;
    future = new ChainedRunnableListenableFuture<Object>(SameThreadSubmitterExecutor.instance(), 
                                                         new TestCallable());
    
    assertFalse(future.isDone());
    future.run();
    assertTrue(future.isDone());
  }
  
  @Test
  public void listenersCalledOnResultTest() {
    listenersCallTest(false);
  }
  
  @Test
  public void listenersCalledOnFailureTest() {
    listenersCallTest(true);
  }
  
  private static void listenersCallTest(final boolean failure) {
    ListenableRunnableFuture<Void> future;
    future = new ChainedRunnableListenableFuture<Void>(SameThreadSubmitterExecutor.instance(), 
                                                       new Callable<Void>() {
      @Override
      public Void call() {
        if (failure) {
          throw new RuntimeException();
        } else {
          return null;
        }
      }
    });
    TestRunnable tr = new TestRunnable();
    future.addListener(tr);
    
    future.run();
    
    assertTrue(tr.ranOnce());
    
    // verify new additions also get called
    tr = new TestRunnable();
    future.addListener(tr, null);
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void addCallbackTest() {
    final String result = StringUtils.randomString(5);
    ListenableRunnableFuture<String> future;
    future = new ChainedRunnableListenableFuture<String>(SameThreadSubmitterExecutor.instance(), 
                                                         new Callable<String>() {
      @Override
      public String call() {
        return result;
      }
    });
    TestFutureCallback tfc = new TestFutureCallback();
    future.addCallback(tfc);
    
    assertEquals(0, tfc.getCallCount());
    
    future.run();
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(result == tfc.getLastResult());
  }
  
  @Test
  public void addCallbackAlreadyDoneFutureTest() {
    final String result = StringUtils.randomString(5);
    ListenableRunnableFuture<String> future;
    future = new ChainedRunnableListenableFuture<String>(SameThreadSubmitterExecutor.instance(), 
                                                         new Callable<String>() {
      @Override
      public String call() {
        return result;
      }
    });
    
    future.run();
    
    TestFutureCallback tfc = new TestFutureCallback();
    future.addCallback(tfc, null);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(result == tfc.getLastResult());
  }
  
  @Test
  public void addCallbackExecutionExceptionTest() {
    final RuntimeException failure = new RuntimeException();
    ListenableRunnableFuture<String> future;
    future = new ChainedRunnableListenableFuture<String>(SameThreadSubmitterExecutor.instance(), 
                                                         new Callable<String>() {
      @Override
      public String call() {
        throw failure;
      }
    });
    
    TestFutureCallback tfc = new TestFutureCallback();
    future.addCallback(tfc);
    
    assertEquals(0, tfc.getCallCount());
    
    future.run();
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(failure == tfc.getLastFailure());
  }
  
  @Test
  public void addCallbackExecutionExceptionAlreadyDoneTest() {
    final RuntimeException failure = new RuntimeException();
    ListenableRunnableFuture<String> future;
    future = new ChainedRunnableListenableFuture<String>(SameThreadSubmitterExecutor.instance(), 
                                                         new Callable<String>() {
      @Override
      public String call() {
        throw failure;
      }
    });
    
    future.run();
    
    TestFutureCallback tfc = new TestFutureCallback();
    future.addCallback(tfc, null);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(failure == tfc.getLastFailure());
  }
}
