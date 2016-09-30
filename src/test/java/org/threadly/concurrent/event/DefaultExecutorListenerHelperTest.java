package org.threadly.concurrent.event;

import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;

@SuppressWarnings("javadoc")
public class DefaultExecutorListenerHelperTest extends ListenerHelperTest {
  @Override
  protected <T> ListenerHelper<T> makeListenerHelper(Class<T> listenerInterface) {
    return new DefaultExecutorListenerHelper<>(listenerInterface, SameThreadSubmitterExecutor.instance());
  }
  
  @Test
  @Override
  @SuppressWarnings({ "unused", "unchecked", "rawtypes" })
  public void constructorFail() {
    try {
      new DefaultExecutorListenerHelper(null, SameThreadSubmitterExecutor.instance());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new DefaultExecutorListenerHelper(ArrayList.class, SameThreadSubmitterExecutor.instance());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new DefaultExecutorListenerHelper(TestInterface.class, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
