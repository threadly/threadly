package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.ThreadlyTester;

@SuppressWarnings("javadoc")
public class DoNothingRunnableTest extends ThreadlyTester {
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
