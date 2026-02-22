package org.threadly.concurrent.event;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;

@SuppressWarnings("javadoc")
public class DefaultExecutorRunnableListenerHelperTest extends RunnableListenerHelperTest {
  @BeforeEach
  @Override
  public void setup() {
    onceHelper = new DefaultExecutorRunnableListenerHelper(true, SameThreadSubmitterExecutor.instance());
    repeatedHelper = new DefaultExecutorRunnableListenerHelper(false, SameThreadSubmitterExecutor.instance());
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
      assertThrows(IllegalArgumentException.class, () -> {
      new DefaultExecutorRunnableListenerHelper(true, null);
      });
  }
  
  @Test
  @Override
  public void listenerExceptionAfterCallTest() {
    // This does not make sense for this type, since listeners are executed asynchronously
  }
}
