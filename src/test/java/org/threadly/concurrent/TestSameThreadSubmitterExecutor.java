package org.threadly.concurrent;

/**
 * Used to make sure {@code SameThreadSubmitterExecutor.instance() ==} checks fail. 
 */
public class TestSameThreadSubmitterExecutor extends SameThreadSubmitterExecutor {
  private static final TestSameThreadSubmitterExecutor INSTANCE = new TestSameThreadSubmitterExecutor();
  
  public static TestSameThreadSubmitterExecutor instance() {
    return INSTANCE;
  }
  
  @SuppressWarnings("deprecation")
  private TestSameThreadSubmitterExecutor() {
    // restrict construction
  }
}
