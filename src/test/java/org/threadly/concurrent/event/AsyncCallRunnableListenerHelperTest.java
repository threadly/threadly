package org.threadly.concurrent.event;

import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;

@SuppressWarnings("javadoc")
public class AsyncCallRunnableListenerHelperTest extends RunnableListenerHelperTest {
  @Before
  @Override
  public void setup() {
    onceHelper = new AsyncCallRunnableListenerHelper(true, SameThreadSubmitterExecutor.instance());
    repeatedHelper = new AsyncCallRunnableListenerHelper(false, SameThreadSubmitterExecutor.instance());
  }
  
  @Test (expected = IllegalArgumentException.class)
  @SuppressWarnings("unused")
  public void constructorFail() {
    new AsyncCallRunnableListenerHelper(true, null);
  }
}
