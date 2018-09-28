package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.threadly.concurrent.future.CancelDebuggingListenableFuture.FutureProcessingStack;

@SuppressWarnings("javadoc")
public class CancelDebuggingListenableFutureTest {
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
      assertEquals(this.getClass().getName(), e.getCause().getStackTrace()[3].getClassName());
    }
  }
}
