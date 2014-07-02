package org.threadly.concurrent.event;

import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;

@SuppressWarnings("javadoc")
public class AsyncCallListenerHelperTest extends ListenerHelperTest {
  @Override
  protected <T> ListenerHelper<T> makeListenerHelper(Class<T> listenerInterface) {
    return AsyncCallListenerHelper.build(listenerInterface, SameThreadSubmitterExecutor.instance());
  }
  
  @Test
  @Override
  @SuppressWarnings({ "unused", "unchecked", "rawtypes" })
  public void constructorFail() {
    try {
      new AsyncCallListenerHelper(null, SameThreadSubmitterExecutor.instance());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new AsyncCallListenerHelper(ArrayList.class, SameThreadSubmitterExecutor.instance());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new AsyncCallListenerHelper(TestInterface.class, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
