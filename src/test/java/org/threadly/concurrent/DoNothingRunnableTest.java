package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.ThreadlyTester;

@SuppressWarnings("javadoc")
public class DoNothingRunnableTest extends ThreadlyTester {
  @Test
  public void staticInstanceTest() {
    assertNotNull(DoNothingRunnable.instance());
  }
  
  @Test
  public void doNothingRun() {
    DoNothingRunnable.instance().run();
    // no exception
  }
}
