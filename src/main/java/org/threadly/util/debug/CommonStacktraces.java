package org.threadly.util.debug;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SingleThreadScheduler;
import org.threadly.util.ExceptionUtils;

/**
 * Class of stack traces that could be used for reference against common situations.
 * 
 * @since 5.25
 */
class CommonStacktraces {
  protected static final ComparableTrace IDLE_THREAD_TRACE_PRIORITY_SCHEDULE1;
  protected static final ComparableTrace IDLE_THREAD_TRACE_PRIORITY_SCHEDULE2;
  protected static final ComparableTrace IDLE_THREAD_TRACE_SINGLE_THREAD_SCHEDULER1;
  protected static final ComparableTrace IDLE_THREAD_TRACE_SINGLE_THREAD_SCHEDULER2;
  protected static final ComparableTrace IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_SYNCHRONOUS_QUEUE;
  protected static final ComparableTrace IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_ARRAY_QUEUE;
  protected static final ComparableTrace IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_LINKED_QUEUE;
  protected static final ComparableTrace IDLE_THREAD_TRACE_SCHEDULED_THREAD_POOL_EXECUTOR1;
  protected static final ComparableTrace IDLE_THREAD_TRACE_SCHEDULED_THREAD_POOL_EXECUTOR2;
  
  static {
    AtomicReference<Thread> psSchedulerThread = new AtomicReference<>();
    AtomicReference<Thread> stsSchedulerThread = new AtomicReference<>();
    AtomicReference<Thread> threadPoolExecutorSynchronousQueueThread = new AtomicReference<>();
    AtomicReference<Thread> threadPoolExecutorArrayBlockingQueueThread = new AtomicReference<>();
    AtomicReference<Thread> threadPoolExecutorLinkedBlockingQueueThread = new AtomicReference<>();
    AtomicReference<Thread> scheduledThreadPoolExecutorThread = new AtomicReference<>();
    PriorityScheduler ps = new PriorityScheduler(1, null, 100, (r) -> {
      Thread t = new Thread(r);
      t.setDaemon(true);
      psSchedulerThread.set(t);
      return t;
    });
    SingleThreadScheduler sts = new SingleThreadScheduler((r) -> {
      Thread t = new Thread(r);
      t.setDaemon(true);
      stsSchedulerThread.set(t);
      return t;
    });
    ThreadPoolExecutor tpeSQ = 
        new ThreadPoolExecutor(1, 1, 100, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), (r) -> {
          Thread t = new Thread(r);
          t.setDaemon(true);
          threadPoolExecutorSynchronousQueueThread.set(t);
          return t;
        });
    ThreadPoolExecutor tpeAQ = 
        new ThreadPoolExecutor(1, 1, 100, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1), (r) -> {
          Thread t = new Thread(r);
          t.setDaemon(true);
          threadPoolExecutorArrayBlockingQueueThread.set(t);
          return t;
        });
    ThreadPoolExecutor tpeLQ = 
        new ThreadPoolExecutor(1, 1, 100, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), (r) -> {
          Thread t = new Thread(r);
          t.setDaemon(true);
          threadPoolExecutorLinkedBlockingQueueThread.set(t);
          return t;
        });
    ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(1, (r) -> {
      Thread t = new Thread(r);
      t.setDaemon(true);
      scheduledThreadPoolExecutorThread.set(t);
      return t;
    });
    try {
      sts.prestartExecutionThread(false);
      ps.prestartAllThreads();
      
      Thread psFirstThread = getParkedThread(psSchedulerThread, null);
      ps.setPoolSize(2);
      ps.prestartAllThreads();
      Thread psSecondThread = getParkedThread(psSchedulerThread, psFirstThread);
      
      Thread stsThread = getParkedThread(stsSchedulerThread, null);
      
      IDLE_THREAD_TRACE_PRIORITY_SCHEDULE1 = new ComparableTrace(psFirstThread.getStackTrace());
      IDLE_THREAD_TRACE_PRIORITY_SCHEDULE2 = new ComparableTrace(psSecondThread.getStackTrace());
      IDLE_THREAD_TRACE_SINGLE_THREAD_SCHEDULER1 = new ComparableTrace(stsThread.getStackTrace());
      
      sts.schedule(DoNothingRunnable.instance(), TimeUnit.HOURS.toMillis(1));
      sts.submit(DoNothingRunnable.instance()).get(); // make sure we execute so the next park is our ideal state
      
      tpeSQ.prestartCoreThread();
      tpeAQ.prestartCoreThread();
      tpeLQ.prestartCoreThread();
      stpe.prestartCoreThread();
      
      Thread sqThread = getParkedThread(threadPoolExecutorSynchronousQueueThread, null);
      Thread aqThread = getParkedThread(threadPoolExecutorArrayBlockingQueueThread, null);
      Thread lqThread = getParkedThread(threadPoolExecutorLinkedBlockingQueueThread, null);
      Thread stpeThread = getParkedThread(scheduledThreadPoolExecutorThread, null);

      IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_SYNCHRONOUS_QUEUE = new ComparableTrace(sqThread.getStackTrace());
      IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_ARRAY_QUEUE = new ComparableTrace(aqThread.getStackTrace());
      IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_LINKED_QUEUE = new ComparableTrace(lqThread.getStackTrace());
      IDLE_THREAD_TRACE_SCHEDULED_THREAD_POOL_EXECUTOR1 = new ComparableTrace(stpeThread.getStackTrace());
      
      stpe.schedule(DoNothingRunnable.instance(), 1, TimeUnit.HOURS);
      stpe.submit(DoNothingRunnable.instance()).get(); // make sure we execute so the next park is our ideal state
      
      while (! isParkedStack(stsThread.getStackTrace())) {
        Thread.yield();
      }
      while (! isParkedStack(stpeThread.getStackTrace())) {
        Thread.yield();
      }
      
      IDLE_THREAD_TRACE_SINGLE_THREAD_SCHEDULER2 = new ComparableTrace(stsThread.getStackTrace());
      IDLE_THREAD_TRACE_SCHEDULED_THREAD_POOL_EXECUTOR2 = new ComparableTrace(stpeThread.getStackTrace());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw ExceptionUtils.makeRuntime(e.getCause());
    } finally {
      try {
        ps.shutdownNow();
      } finally {
        try {
          sts.shutdownNow();
        } finally {
          try {
            tpeSQ.shutdownNow();
          } finally {
            try {
              tpeAQ.shutdownNow();
            } finally {
              try {
                tpeLQ.shutdownNow();
              } finally {
                stpe.shutdownNow();
              }
            }
          }
        }
      }
    }
  }
  
  private static Thread getParkedThread(AtomicReference<Thread> threadReference, Thread not) {
    Thread thread;
    StackTraceElement[] stackTrace;
    while (true) {
      if ((thread = threadReference.get()) != not && thread.isAlive()) {
        stackTrace = thread.getStackTrace();
        if (isParkedStack(stackTrace)) {
          break;
        }
      }
      Thread.yield();
    }
    return thread;
  }
  
  private static boolean isParkedStack(StackTraceElement[] stackTrace) {
    return stackTrace.length > 1 && stackTrace[0].getMethodName().equals("park");
  }
}
