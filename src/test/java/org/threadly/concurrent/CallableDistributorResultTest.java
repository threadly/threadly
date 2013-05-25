package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.threadly.concurrent.CallableDistributor.Result;

@SuppressWarnings("javadoc")
public class CallableDistributorResultTest {
  private static final String successStr = "forTheWin!";
  private static final Result<String> successResul = new Result<String>(successStr);
  private static final Exception failureException = new Exception();
  private static final Result<String> failureResul = new Result<String>(failureException);
  
  @Test
  public void getSuccessTest() throws ExecutionException {
    assertTrue(successResul.get() == successStr);
  }

  @Test
  public void getExecutionExceptionTest() {
    try {
      failureResul.get();
      fail("Exception should have been thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause().equals(failureException));
    }
  }
  
  @Test
  public void getResultTest() {
    assertTrue(successResul.getResult() == successStr);
    assertNull(failureResul.getResult());
  }
  
  @Test
  public void getFailureTest() {
    assertTrue(failureResul.getFailure() == failureException);
    assertNull(successResul.getFailure());
  }
  
}
