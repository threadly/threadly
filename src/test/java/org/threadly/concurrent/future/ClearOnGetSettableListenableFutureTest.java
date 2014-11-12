package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class ClearOnGetSettableListenableFutureTest extends SettableListenableFutureTest {
  @Before
  @Override
  public void setup() {
    slf = new ClearOnGetSettableListenableFuture<String>();
  }
  
  @Test (expected = IllegalStateException.class)
  public void getTwiceFail() throws InterruptedException, ExecutionException {
    slf.setResult(null);
    slf.get();
    
    slf.get();
    fail("Exception should have thrown");
  }
  
  @Test (expected = IllegalStateException.class)
  public void getTimeoutTwiceFail() throws InterruptedException, ExecutionException, TimeoutException {
    slf.setResult(null);
    slf.get(1000, TimeUnit.MILLISECONDS);
    
    slf.get(1000, TimeUnit.MILLISECONDS);
    fail("Exception should have thrown");
  }
  
  @Test
  public void getAfterTimeoutThrowTest() throws InterruptedException, ExecutionException {
    try {
      slf.get(1, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (TimeoutException e) {
      // expected
    }
    
    String result = StringUtils.randomString(5);
    slf.setResult(result);
    
    assertTrue(slf.get() == result);
  }
}
