package org.threadly.concurrent.future;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.SameThreadSubmitterExecutor;

@SuppressWarnings("javadoc")
public class ImmediateResultListenableFutureTest extends ThreadlyTester {
  @Test
  public void nullResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ImmediateListenableFutureTest.resultTest(new ImmediateResultListenableFuture<>(null), null);
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.NULL_RESULT, null);
  }
  
  @Test
  public void booleanResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.BOOLEAN_FALSE_RESULT, false);
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.BOOLEAN_TRUE_RESULT, true);
  }
  
  @Test
  public void emptyStringResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.EMPTY_STRING_RESULT, "");
  }
  
  @Test
  public void emptyOptionalResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.EMPTY_OPTIONAL_RESULT, 
                                             Optional.empty());
  }
  
  @Test
  public void emptyListResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.EMPTY_LIST_RESULT, 
                                             Collections.emptyList());
  }
  
  @Test
  public void emptyMapResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.EMPTY_MAP_RESULT, 
                                             Collections.emptyMap());
  }
  
  @Test
  public void emptySetResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.EMPTY_SET_RESULT, 
                                             Collections.emptySet());
  }
  
  @Test
  public void emptySortedMapResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.EMPTY_SORTED_MAP_RESULT, 
                                             Collections.emptySortedMap());
  }
  
  @Test
  public void emptySort6edSetResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.EMPTY_SORTED_SET_RESULT, 
                                             Collections.emptySortedSet());
  }
  
  @Test
  public void emptyIteratorResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.EMPTY_ITERATOR_RESULT, 
                                             Collections.emptyIterator());
  }
  
  @Test
  public void emptyListIteratorResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.EMPTY_LIST_ITERATOR_RESULT, 
                                             Collections.emptyListIterator());
  }
  
  @Test
  public void emptyEnumerationResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.EMPTY_ENUMERATION_RESULT, 
                                             Collections.emptyEnumeration());
  }
  
  @Test
  public void nonNullResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    Object result = new Object();
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<>(result);
    
    ImmediateListenableFutureTest.resultTest(testFuture, result);
  }
  
  @Test
  public void cancelTest() {
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<>(null);

    ImmediateListenableFutureTest.cancelTest(testFuture);
  }
  
  @Test
  public void listenerTest() {
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<>(null);
    
    ImmediateListenableFutureTest.listenerTest(testFuture);
  }
  
  @Test
  public void callbackTest() {
    Object result = new Object();
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<>(result);
    
    ImmediateListenableFutureTest.resultCallbackTest(testFuture, result);
  }
  
  @Test
  public void failureCallbackTest() {
    Object result = new Object();
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<>(result);
    
    AtomicBoolean ran = new AtomicBoolean();
    assertTrue(testFuture == testFuture.failureCallback((ignored) -> ran.set(true)));
    assertFalse(ran.get());
  }
  
  @Test
  public void mapFailureTest() {
    // should be straight through
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<>(null);
    
    assertTrue(testFuture == testFuture.mapFailure(Exception.class, 
                                                   (t) -> { throw new RuntimeException(); }));
    assertTrue(testFuture == testFuture.mapFailure(Exception.class, 
                                                   (t) -> { throw new RuntimeException(); }, 
                                                   SameThreadSubmitterExecutor.instance()));
    assertTrue(testFuture == testFuture.mapFailure(Exception.class, 
                                                   (t) -> { throw new RuntimeException(); }, 
                                                   SameThreadSubmitterExecutor.instance(), null));
  }
  
  @Test
  public void flatMapFailureTest() {
    // should be straight through
    ListenableFuture<Object> testFuture = new ImmediateResultListenableFuture<>(null);
    
    assertTrue(testFuture == testFuture.flatMapFailure(Exception.class, 
                                                       (t) -> FutureUtils.immediateFailureFuture(new RuntimeException())));
    assertTrue(testFuture == testFuture.flatMapFailure(Exception.class, 
                                                       (t) -> FutureUtils.immediateFailureFuture(new RuntimeException()), 
                                                       SameThreadSubmitterExecutor.instance()));
    assertTrue(testFuture == testFuture.flatMapFailure(Exception.class, 
                                                       (t) -> FutureUtils.immediateFailureFuture(new RuntimeException()), 
                                                       SameThreadSubmitterExecutor.instance(), null));
  }
  
  @Test
  public void getRunningStackTraceTest() {
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<>(null);
    
    ImmediateListenableFutureTest.getRunningStackTraceTest(testFuture);
  }
}
