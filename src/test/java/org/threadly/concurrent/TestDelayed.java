package org.threadly.concurrent;

import java.util.concurrent.TimeUnit;

@SuppressWarnings("javadoc")
public class TestDelayed extends AbstractDelayed {
  protected long delayInMs;
  
  public TestDelayed(long delayInMs) {
    this.delayInMs = delayInMs;
  }
  
  public void setDelay(long delayInMs) {
    this.delayInMs = delayInMs;
  }
  
  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(delayInMs, TimeUnit.MILLISECONDS);
  }
  
  @Override
  public String toString() {
    return "d:" + delayInMs;
  }
  
  @Override
  public int hashCode() {
    return toString().hashCode();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof TestDelayed) {
      TestDelayed td = (TestDelayed)o;
      return this.toString().equals(td.toString());
    } else {
      return false;
    }
  }
}
