package org.threadly.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
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
