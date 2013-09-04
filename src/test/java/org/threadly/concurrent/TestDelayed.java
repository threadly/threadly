package org.threadly.concurrent;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("javadoc")
public class TestDelayed implements Delayed {
  protected final long delayInMs;
  
  public TestDelayed(long delayInMs) {
    this.delayInMs = delayInMs;
  }
  
  @Override
  public int compareTo(Delayed o) {
    if (this == o) {
      return 0;
    } else {
      long thisDelay = this.getDelay(TimeUnit.MILLISECONDS);
      long otherDelay = o.getDelay(TimeUnit.MILLISECONDS);
      if (thisDelay == otherDelay) {
        return 0;
      } else if (thisDelay > otherDelay) {
        return 1;
      } else {
        return -1;
      }
    }
  }
  
  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(delayInMs, 
                        TimeUnit.MILLISECONDS);
  }
  
  @Override
  public String toString() {
    return "d:" + delayInMs;
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
