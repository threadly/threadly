package org.threadly.concurrent;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
/**
 * <p>Since {@link Delayed} requires a {@code compareTo(Delayed)} implementation which should be 
 * the same for all implementations.  This abstract class provides a way to reduce code 
 * duplication.</p>
 * 
 * @deprecated No direct replacement
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
@Deprecated
public abstract class AbstractDelayed implements Delayed {
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
}
