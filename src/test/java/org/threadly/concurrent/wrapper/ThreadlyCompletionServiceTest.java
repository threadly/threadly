package org.threadly.concurrent.wrapper;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;
import org.threadly.util.Clock;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class ThreadlyCompletionServiceTest extends ThreadlyTester {
  private TestableScheduler scheduler;
  private ThreadlyCompletionService<String> completionService;
  
  @Before
  public void setup() {
    scheduler = new TestableScheduler();
    completionService = new ThreadlyCompletionService<>(scheduler);
  }
  
  @After
  public void cleanup() {
    scheduler = null;
    completionService = null;
  }
  
  @Test
  public void simpleSubmitTest() throws InterruptedException, ExecutionException {
    TestRunnable tr = new TestRunnable();
    String result = StringUtils.makeRandomString(5);
    
    ListenableFuture<String> submitFuture = completionService.submit(tr, result);
    assertFalse(submitFuture.isDone());
    assertNull(completionService.poll());
    
    assertEquals(1, scheduler.tick());
    assertTrue(submitFuture.isDone());
    
    assertTrue(submitFuture == completionService.poll()) ;
    assertNull(completionService.poll()); // should now be removed
    assertEquals(result, submitFuture.get());
  }
  
  @Test
  public void pollTimeoutTest() throws InterruptedException {
    completionService.submit(DoNothingRunnable.instance(), null);
    
    long start = Clock.accurateForwardProgressingMillis();
    assertNull(completionService.poll(DELAY_TIME, TimeUnit.MILLISECONDS));
    long end = Clock.accurateForwardProgressingMillis();
    
    assertTrue(end - start >= DELAY_TIME);
  }
  
  @Test
  public void takeTest() throws InterruptedException {
    ListenableFuture<String> submitFuture = 
        completionService.submit(DoNothingRunnable.instance(), null);
    assertEquals(1, scheduler.tick());
    
    assertTrue(submitFuture == completionService.take());
  }
}
