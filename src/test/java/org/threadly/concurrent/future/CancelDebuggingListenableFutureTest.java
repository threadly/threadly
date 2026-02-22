package org.threadly.concurrent.future;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;
import org.threadly.concurrent.future.CancelDebuggingListenableFuture.FutureProcessingStack;

@SuppressWarnings("javadoc")
public class CancelDebuggingListenableFutureTest extends ListenableFutureInterfaceTest {
  @Override
  protected ListenableFutureFactory makeListenableFutureFactory() {
    return new CancelDebuggingListenableFutureFactory();
  }
  
  @Test
  public void notStartedTest() throws InterruptedException, ExecutionException {
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    CancelDebuggingListenableFuture<Object> debugFuture = new CancelDebuggingListenableFuture<>(slf);
    
    assertTrue(debugFuture.cancel(false));
    
    try {
      debugFuture.get();
      fail("Exception should have thrown");
    } catch (CancellationException e) {
      // expected
      assertNull(e.getCause());
    }
  }

  @Test
  public void withRunningStackTest() throws InterruptedException, ExecutionException {
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    slf.setRunningThread(Thread.currentThread());
    CancelDebuggingListenableFuture<Object> debugFuture = new CancelDebuggingListenableFuture<>(slf);
    
    assertTrue(debugFuture.cancel(false));
    
    try {
      debugFuture.get();
      fail("Exception should have thrown");
    } catch (CancellationException e) {
      // expected
      assertNotNull(e.getCause());
      assertTrue(e.getCause() instanceof FutureProcessingStack);
      assertEquals(this.getClass().getName(), e.getCause().getStackTrace()[4].getClassName());
    }
  }
  
  private static class CancelDebuggingListenableFutureFactory implements ListenableFutureFactory {
    @Override
    public CancelDebuggingListenableFuture<?> makeCanceled() {
      SettableListenableFuture<?> slf = new SettableListenableFuture<>();
      slf.cancel(false);
      return new CancelDebuggingListenableFuture<>(slf);
    }
    
    @Override
    public CancelDebuggingListenableFuture<Object> makeWithFailure(Exception e) {
      SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
      slf.handleFailure(e);
      return new CancelDebuggingListenableFuture<>(slf);
    }

    @Override
    public <T> CancelDebuggingListenableFuture<T> makeWithResult(T result) {
      SettableListenableFuture<T> slf = new SettableListenableFuture<>();
      slf.handleResult(result);
      return new CancelDebuggingListenableFuture<>(slf);
    }
  }
}
