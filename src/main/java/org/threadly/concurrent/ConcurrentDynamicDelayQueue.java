package org.threadly.concurrent;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.util.ListUtils;

/**
 * This is a DynamicDelayQueue that is thread safe by use of the 
 * ConcurrentArrayList.  It currently is better performing in most 
 * cases compared to the SynchronizedDynamicDelayQueue.
 * 
 * @author jent - Mike Jensen
 * @param <T> Parameter to indicate what type of item is contained in the queue
 */
public class ConcurrentDynamicDelayQueue<T extends Delayed> implements DynamicDelayQueue<T> {
  private static final int SPIN_LOCK_THRESHOLD = 5;
  private static final int QUEUE_FRONT_PADDING = 0;
  private static final int QUEUE_REAR_PADDING = 0;
  
  private final boolean randomAccessQueue;
  private final VirtualLock queueLock;
  private final ConcurrentArrayList<T> queue;

  /**
   * Constructs a new concurrent queue.
   */
  public ConcurrentDynamicDelayQueue() {
    this(new NativeLock());
  }

  /**
   * Constructs a queue, providing the lock that will be called 
   * on with .await().  Thus it allows you to synchronize around
   * the .take() and have the lock released while the thread blocks.
   * 
   * @param queueLock lock that is used internally
   */
  protected ConcurrentDynamicDelayQueue(VirtualLock queueLock) {
    queue = new ConcurrentArrayList<T>(queueLock, 
                                       QUEUE_FRONT_PADDING, 
                                       QUEUE_REAR_PADDING);
    randomAccessQueue = (queue instanceof RandomAccess);
    this.queueLock = queueLock;
  }
  
  @Override
  public String toString() {
    return "Queue:" + queue.toString();
  }
  
  @Override
  public Object getLock() {
    return queueLock;
  }
  
  @Override
  public void sortQueue() {
    synchronized (queueLock) {
      Collections.sort(queue);
      
      queueLock.signalAll();
    }
  }
  
  @Override
  public boolean add(T e) {
    if (e == null) {
      return false;
    }
    
    synchronized (queueLock) {
      int insertionIndex = ListUtils.getInsertionEndIndex(queue, e, randomAccessQueue);
      
      queue.add(insertionIndex, e);
      
      queueLock.notify();
    }
    
    return true;
  }

  @Override
  public void reposition(T e) {
    if (e == null) {
      return;
    }

    synchronized (queueLock) {
      int insertionIndex = ListUtils.getInsertionEndIndex(queue, e, randomAccessQueue);
      
      /* provide the option to search backwards since the item 
       * will most likely be towards the back of the queue */
      queue.reposition(e, insertionIndex, true);
      
      queueLock.signalAll();
    }
  }

  @Override
  public void addLast(T e) {
    if (e == null) {
      throw new NullPointerException();
    }
    
    queue.add(e);
  }

  @Override
  public T element() {
    T result = peek();
    if (result == null) {
      throw new NoSuchElementException();
    }
    
    return result;
  }

  @Override
  public boolean offer(T e) {
    return add(e);
  }

  @Override
  public T peek() {
    T next = queue.peek();
    
    if (next != null && next.getDelay(TimeUnit.MILLISECONDS) > 0) {
      next = null;
    }
    
    return next;
  }

  @Override
  public T poll() {
    T next = queue.peek();
    if (next != null && next.getDelay(TimeUnit.MILLISECONDS) <= 0) {
      // we likely can win, so lets try
      synchronized (queueLock) {
        if ((next = queue.peek()) != null && 
            next.getDelay(TimeUnit.MILLISECONDS) <= 0) {
          return queue.remove(0);
        } else {
          return null;
        }
      }
    } else {
      return null;
    }
  }
  
  protected T blockTillAvailable() throws InterruptedException {
    while (true) { // will break out when ready
      T next = queue.peek();
      if (next == null) {
        synchronized (queueLock) {
          while ((next = queue.peek()) == null) {
            queueLock.await();
          }
        }
      }
      
      long nextDelay = next.getDelay(TimeUnit.MILLISECONDS);
      if (nextDelay > 0) {
        if (nextDelay > SPIN_LOCK_THRESHOLD) {
          synchronized (queueLock) {
            if (queue.peek() == next) {
              queueLock.await(nextDelay);
            } else {
              continue; // start form the beginning
            }
          }
        } else {
          long startTime = ClockWrapper.getAccurateTime();
          long startDelay = nextDelay;
          while ((next = queue.peek()) != null && 
                 (nextDelay = next.getDelay(TimeUnit.MILLISECONDS)) > 0 && 
                 (nextDelay != startDelay || 
                    ClockWrapper.getAccurateTime() < startTime + SPIN_LOCK_THRESHOLD)) {
            // spin
          }
          if (nextDelay <= 0) {
            return next;
          } else if (next != null) {
            /* clock is advancing but delay is not, so since this is not
             * a real time delay we just need to wait
             */
            synchronized (queueLock) {
              if (next == queue.peek()) {
                queueLock.await(nextDelay);
              } else {
                // loop
              }
            }
          }
        }
      } else {
        return next;
      }
    }
  }

  @Override
  public T take() throws InterruptedException {
    T next = blockTillAvailable();
    synchronized (queueLock) {
      if (next == queue.peek()) {
        queue.remove(0);
      } else {
        next = blockTillAvailable();
        queue.remove(0);
      }
    }
   
    return next;
  }

  @Override
  public T remove() {
    T result = poll();
    if (result == null) {
      throw new NoSuchElementException();
    }
    
    return result;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    synchronized (queueLock) {
      Iterator<? extends T> it = c.iterator();
      boolean added = it.hasNext();
      while (it.hasNext()) {
        add(it.next());
      }
      
      return added;
    }
  }

  @Override
  public void clear() {
    queue.clear();
  }

  @Override
  public boolean contains(Object o) {
    return queue.contains(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return queue.containsAll(c);
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public Iterator<T> iterator() {
    if (! Thread.holdsLock(queueLock)) {
      throw new IllegalStateException("Must have lock in order to get iterator");
    }
    
    return queue.iterator();
  }

  @Override
  public ConsumerIterator<T> consumeIterator() throws InterruptedException {
    if (! Thread.holdsLock(queueLock)) {
      throw new IllegalStateException("Must have lock in order to get iterator");
    }
    
    blockTillAvailable();
    
    return new ConsumerIterator<T>() {
      private T next = null;
      
      @Override
      public boolean hasNext() {
        if (next == null) {
          next = ConcurrentDynamicDelayQueue.this.peek();
        }
        
        return next != null;
      }
      
      @Override
      public T peek() {
        if (next == null) {
          next = ConcurrentDynamicDelayQueue.this.peek();
        }
        
        return next;
      }

      @Override
      public T remove() {
        T result;
        if (next != null) {
          result = next;
          queue.remove(result);
          next = null;
        } else {
          result = ConcurrentDynamicDelayQueue.this.remove();
        }
        
        return result;
      }
    };
  }

  @Override
  public boolean remove(Object o) {
    return queue.remove(o);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return queue.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return queue.retainAll(c);
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public Object[] toArray() {
    return queue.toArray();
  }

  @Override
  public <E> E[] toArray(E[] a) {
    return queue.toArray(a);
  }
}
