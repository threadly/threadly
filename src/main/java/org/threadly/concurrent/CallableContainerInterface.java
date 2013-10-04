package org.threadly.concurrent;

import java.util.concurrent.Callable;

public interface CallableContainerInterface<T> {
  public Callable<T> getContainedCallable();
}
