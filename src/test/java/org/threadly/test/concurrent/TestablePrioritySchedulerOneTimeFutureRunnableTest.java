package org.threadly.test.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.PriorityScheduledExecutor;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.FutureTest;
import org.threadly.concurrent.future.FutureTest.FutureFactory;
import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.NativeLockFactory;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.test.concurrent.TestablePriorityScheduler.OneTimeFutureRunnable;

@SuppressWarnings("javadoc")
public class TestablePrioritySchedulerOneTimeFutureRunnableTest {
  private static final Executor executor = Executors.newFixedThreadPool(10);
  
  @Test
  public void blockTillCompletedTest() {
    FutureTest.blockTillCompletedTest(new Factory());
  }
  
  @Test
  public void blockTillCompletedFail() {
    FutureTest.blockTillCompletedFail(new Factory());
  }
  
  @Test
  public void getTimeoutFail() throws InterruptedException, ExecutionException {
    FutureTest.getTimeoutFail(new Factory());
  }
  
  @Test
  public void cancelTest() {
    FutureTest.cancelTest(new Factory());
  }
  
  @Test
  public void isDoneTest() {
    FutureTest.isDoneTest(new Factory());
  }
  
  @Test
  public void isDoneFail() {
    FutureTest.isDoneFail(new Factory());
  }
  
  @Test
  public void listenerTest() {
    TestRunnable tr = new TestRunnable();
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(1, 1, 200);
    try {
      TestablePriorityScheduler scheduler = new TestablePriorityScheduler(executor, 
                                                                          TaskPriority.High);
      OneTimeFutureRunnable<Object> future = scheduler.new OneTimeFutureRunnable<Object>(tr, null, 0, 
                                                                                         TaskPriority.High, 
                                                                                         new NativeLock());
      
      assertEquals(future.listeners.size(), 0); // empty to start
      
      TestRunnable listener = new TestRunnable();
      
      future.addListener(listener);
      
      assertEquals(future.listeners.size(), 1); // should now have once now that the runnable has not run yet
      
      future.run(new NativeLockFactory()); // this should call the listener
      scheduler.tick();
      
      assertTrue(listener.ranOnce()); // verify listener was called
      
      assertEquals(future.listeners.size(), 0); // empty after listener calls
      
      TestRunnable postRunListener = new TestRunnable();
      
      future.addListener(postRunListener);
      scheduler.tick();
      
      assertTrue(postRunListener.ranOnce()); // verify listener was called
      
      assertEquals(future.listeners.size(), 0); // still empty after future ran
      
      // verify run on correct executor
      TestRunnable executorListener = new TestRunnable();
      TestExecutor testExecutor = new TestExecutor();
      future.addListener(executorListener, testExecutor);
      
      assertEquals(testExecutor.providedRunnables.size(), 1);
      assertTrue(testExecutor.providedRunnables.get(0) == executorListener);
    } finally {
      executor.shutdown();
    }
  }
  
  private class Factory implements FutureFactory {
    @Override
    public Future<?> make(Runnable run, VirtualLock lock) {
      return new FutureRunnable<Object>(run, lock);
    }

    @Override
    public <T> Future<T> make(Callable<T> callable, VirtualLock lock) {
      return new FutureRunnable<T>(callable, lock);
    }
  }
  
  private class FutureRunnable<T> implements Future<T>, Runnable {
    private final TestablePriorityScheduler scheduler;
    private final OneTimeFutureRunnable<T> otfr;
    
    private FutureRunnable(Runnable run, VirtualLock lock) {
      scheduler = new TestablePriorityScheduler(executor, 
                                                TaskPriority.High);
      this.otfr = scheduler.new OneTimeFutureRunnable<T>(run, null, 0, 
                                                         TaskPriority.High, 
                                                         lock);
    }
    
    private FutureRunnable(Callable<T> callable, VirtualLock lock) {
      scheduler = new TestablePriorityScheduler(executor, 
                                                TaskPriority.High);
      this.otfr = scheduler.new OneTimeFutureRunnable<T>(callable, 0, 
                                                         TaskPriority.High, 
                                                         lock);
    }

    @Override
    public void run() {
      otfr.run(scheduler);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return otfr.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return otfr.isCancelled();
    }

    @Override
    public boolean isDone() {
      return otfr.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      return otfr.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException,
                                                     ExecutionException,
                                                     TimeoutException {
      return otfr.get(timeout, unit);
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
