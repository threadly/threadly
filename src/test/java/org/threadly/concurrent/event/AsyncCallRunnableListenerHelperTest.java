package org.threadly.concurrent.event;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;

@SuppressWarnings("javadoc")
public class AsyncCallRunnableListenerHelperTest extends RunnableListenerHelperTest {
  @BeforeEach
  @Override
  public void setup() {
    onceHelper = new AsyncCallRunnableListenerHelper(true, SameThreadSubmitterExecutor.instance());
    repeatedHelper = new AsyncCallRunnableListenerHelper(false, SameThreadSubmitterExecutor.instance());
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
      assertThrows(IllegalArgumentException.class, () -> {
      new AsyncCallRunnableListenerHelper(true, null);
      });
  }
}
