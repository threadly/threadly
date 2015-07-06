package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class DoNothingRunnableTest {
  @Test
  public void doNothingRun() {
    new DoNothingRunnable().run();
    // no exception
  }
  
  @Test
  public void staticInstanceTest() {
    assertNotNull(DoNothingRunnable.instance());
  }
}
