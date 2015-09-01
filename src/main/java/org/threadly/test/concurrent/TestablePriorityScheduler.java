package org.threadly.test.concurrent;

import org.threadly.concurrent.PrioritySchedulerInterface;
/**
 * <p>This is similar to {@link TestableScheduler} except that it implements the 
 * {@link PrioritySchedulerInterface}.  This allows you to use a this testable implementation in 
 * code which requires a {@link PrioritySchedulerInterface}.</p>
 * 
 * @deprecated Use {@link TestableScheduler} now that it also implements {@link PrioritySchedulerInterface}
 * 
 * @author jent - Mike Jensen
 * @since 3.4.0
 */
@Deprecated
public class TestablePriorityScheduler extends TestableScheduler 
                                       implements PrioritySchedulerInterface {
  // nothing added here
}
