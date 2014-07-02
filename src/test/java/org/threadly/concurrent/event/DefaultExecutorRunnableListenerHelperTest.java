package org.threadly.concurrent.event;

import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;

@SuppressWarnings("javadoc")
public class DefaultExecutorRunnableListenerHelperTest extends RunnableListenerHelperTest {
  @Before
  @Override
  public void setup() {
    onceHelper = new DefaultExecutorRunnableListenerHelper(true, SameThreadSubmitterExecutor.instance());
    repeatedHelper = new DefaultExecutorRunnableListenerHelper(false, SameThreadSubmitterExecutor.instance());
  }
  
  @Test (expected = IllegalArgumentException.class)
  @SuppressWarnings("unused")
  public void constructorFail() {
    new DefaultExecutorRunnableListenerHelper(true, null);
  }
  
  @Test
  @Override
  public void listenerExceptionAfterCallTest() {
    // This does not make sense for this type, since listeners are executed asynchronously
  }
}
