package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.future.RunnableFutureTest.BlockingFutureFactory;
import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ListenableFutureVirtualTaskTest {
  @Test
  public void blockTillCompletedTest() {
    RunnableFutureTest.blockTillCompletedTest(new Factory());
  }
  
  @Test
  public void blockTillCompletedFail() {
    RunnableFutureTest.blockTillCompletedFail(new Factory());
  }
  
  @Test
  public void getTimeoutFail() throws InterruptedException, ExecutionException {
    RunnableFutureTest.getTimeoutFail(new Factory());
  }
  
  @Test
  public void cancelTest() {
    RunnableFutureTest.cancelTest(new Factory());
  }
  
  @Test
  public void isDoneTest() {
    RunnableFutureTest.isDoneTest(new Factory());
  }
  
  @Test
  public void isDoneFail() {
    RunnableFutureTest.isDoneFail(new Factory());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addListenerFail() {
    ListenableFutureVirtualTask<Object> future = new ListenableFutureVirtualTask<Object>(new TestRunnable(), null, new NativeLock());
    
    future.addListener(null);
    fail("Exception should have thrown");
  }
  
  @Test
  public void listenerTest() {
    TestRunnable tr = new TestRunnable();
    
    ListenableFutureVirtualTask<Object> future = new ListenableFutureVirtualTask<Object>(tr, null, new NativeLock());
    
    assertEquals(future.listeners.size(), 0); // empty to start
    
    TestRunnable listener = new TestRunnable();
    
    future.addListener(listener);
    
    assertEquals(future.listeners.size(), 1); // should now have once now that the runnable has not run yet
    
    future.run(); // this should call the listener
    
    assertTrue(listener.ranOnce()); // verify listener was called
    
    assertEquals(future.listeners.size(), 0); // empty after listener calls
    
    TestRunnable postRunListener = new TestRunnable();
    
    future.addListener(postRunListener);
    
    assertTrue(postRunListener.ranOnce()); // verify listener was called
    
    assertEquals(future.listeners.size(), 0); // still empty after future ran
    
    // verify run on correct executor
    TestRunnable executorListener = new TestRunnable();
    TestExecutor executor = new TestExecutor();
    future.addListener(executorListener, executor);
    
    assertEquals(executor.providedRunnables.size(), 1);
    assertTrue(executor.providedRunnables.get(0) == executorListener);
  }
  
  @Test
  public void listenerExceptionAddBeforeRunTest() {
    TestRunnable listener = new TestRunnable() {
      @Override
      public void handleRunFinish() {
        throw new RuntimeException();
      }
    };
    
    ListenableFutureVirtualTask<Object> future = new ListenableFutureVirtualTask<Object>(new TestRunnable(), null, new NativeLock());
    
    future.addListener(listener);
    future.run();
    
    assertTrue(listener.ranOnce());
  }
  
  @Test
  public void listenerExceptionAddAfterRunTest() {
    TestRunnable listener = new TestRunnable() {
      @Override
      public void handleRunFinish() {
        throw new RuntimeException();
      }
    };
    
    ListenableFutureVirtualTask<Object> future = new ListenableFutureVirtualTask<Object>(new TestRunnable(), null, new NativeLock());
    
    future.run();
    try {
      future.addListener(listener);
      fail("Exception should have thrown");
    } catch (RuntimeException e) {
      // expected
    }
    
    assertTrue(listener.ranOnce());
  }
  
  @Test (expected = CancellationException.class)
  public void getStaticCancelationExceptionTest() throws InterruptedException, ExecutionException {
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunFinish() {
        throw StaticCancellationException.instance();
      }
    };
    
    ListenableFutureVirtualTask<Object> future = new ListenableFutureVirtualTask<Object>(tr, null, new NativeLock());
    
    future.run();
    future.get();
  }
  
  @Test (expected = CancellationException.class)
  public void getWithTimeoutStaticCancelationExceptionTest() throws InterruptedException, ExecutionException, TimeoutException {
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunFinish() {
        throw StaticCancellationException.instance();
      }
    };
    
    ListenableFutureVirtualTask<Object> future = new ListenableFutureVirtualTask<Object>(tr, null, new NativeLock());
    
    future.run();
    future.get(100, TimeUnit.MILLISECONDS);
  }
  
  @Test (expected = CancellationException.class)
  public void getCancelationExceptionTest() throws InterruptedException, ExecutionException {
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunFinish() {
        throw new CancellationException();
      }
    };
    
    ListenableFutureVirtualTask<Object> future = new ListenableFutureVirtualTask<Object>(tr, null, new NativeLock());
    
    future.run();
    future.get();
  }
  
  @Test (expected = CancellationException.class)
  public void getWithTimeoutCancelationExceptionTest() throws InterruptedException, ExecutionException, TimeoutException {
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunFinish() {
        throw new CancellationException();
      }
    };
    
    ListenableFutureVirtualTask<Object> future = new ListenableFutureVirtualTask<Object>(tr, null, new NativeLock());
    
    future.run();
    future.get(100, TimeUnit.MILLISECONDS);
  }
  
  @Test (expected = ExecutionException.class)
  public void getExecutionExceptionTest() throws InterruptedException, ExecutionException {
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunFinish() {
        throw new RuntimeException();
      }
    };
    
    ListenableFutureVirtualTask<Object> future = new ListenableFutureVirtualTask<Object>(tr, null, new NativeLock());
    
    future.run();
    future.get();
  }
  
  @Test (expected = ExecutionException.class)
  public void getWithTimeoutExecutionExceptionTest() throws InterruptedException, ExecutionException, TimeoutException {
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunFinish() {
        throw new RuntimeException();
      }
    };
    
    ListenableFutureVirtualTask<Object> future = new ListenableFutureVirtualTask<Object>(tr, null, new NativeLock());
    
    future.run();
    future.get(100, TimeUnit.MILLISECONDS);
  }
  
  private class Factory implements BlockingFutureFactory {
    @Override
    public RunnableFuture<?> make(Runnable run, VirtualLock lock) {
      return new ListenableFutureVirtualTask<Object>(run, null, lock);
    }

    @Override
    public <T> RunnableFuture<T> make(Callable<T> callable, VirtualLock lock) {
      return new ListenableFutureVirtualTask<T>(callable, lock);
    }

    @Override
    public RunnableFuture<?> make(Runnable run) {
      return new ListenableFutureVirtualTask<Object>(run, null, 
                                                     new NativeLock());
    }

    @Override
    public <T> RunnableFuture<T> make(Callable<T> callable) {
      return new ListenableFutureVirtualTask<T>(callable, 
                                                new NativeLock());
    }
  }
  
  private class TestExecutor implements Executor {
    public List<Runnable> providedRunnables = new LinkedList<Runnable>();
    
    @Override
    public void execute(Runnable command) {
      providedRunnables.add(command);
    }
  }
}
