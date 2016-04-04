package org.threadly.concurrent.statistics;

/**
 * <p>An extension of {@link StatisticExecutor}, defining specific behavior when the statistic 
 * tracker is implementing for a scheduler which has a concept of task priorities.</p>
 * 
 * @deprecated Moved to {@link org.threadly.concurrent.statistic.StatisticPriorityScheduler}
 * 
 * @author jent - Mike Jensen
 * @since 4.5.0
 */
@Deprecated
public interface StatisticPriorityScheduler 
                     extends StatisticExecutor, 
                             org.threadly.concurrent.statistic.StatisticPriorityScheduler {
  // nothing added here, this has entirely moved
}
