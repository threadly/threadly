package org.threadly.concurrent;

import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class TestRuntimeFailureRunnable extends TestRunnable {
  private final RuntimeException toThrowException;
  
  public TestRuntimeFailureRunnable() {
    this(null);
  }
  
  public TestRuntimeFailureRunnable(RuntimeException toThrowException) {
    this.toThrowException = toThrowException;
  }

  @Override
  protected void handleRunFinish() {
    if (toThrowException != null) {
      throw toThrowException;
    } else {
      throw new RuntimeException();
    }
  }
}