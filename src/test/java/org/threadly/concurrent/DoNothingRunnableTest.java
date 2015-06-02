package org.threadly.concurrent;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class DoNothingRunnableTest {
  @Test
  public void doNothingRun() {
    new DoNothingRunnable().run();
    // no exception
  }
}
