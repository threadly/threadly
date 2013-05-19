package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.util.ListUtils;

public class VolatileDynamicDelayQueue<T extends Delayed> implements DynamicDelayQueue<T> {
  private static final Logger log = Logger.getLogger(VolatileDynamicDelayQueue.class.getSimpleName());
  private static final boolean VERBOSE = false; // TODO - remove debug logging

  private final boolean randomAccessQueue;
  private final VirtualLock queueLock;
  private volatile List<T> lastQueueReference;
  
  public VolatileDynamicDelayQueue() {
    this(new NativeLock());
  }
  
  protected VolatileDynamicDelayQueue(VirtualLock queueLock) {
    lastQueueReference = new ArrayList<T>(0);
    randomAccessQueue = (lastQueueReference instanceof RandomAccess);
    this.queueLock = queueLock;
  }
  
  @Override
  public Object getLock() {
    return queueLock;
  }
  
  @Override
  public void sortQueue() {
    synchronized (queueLock) {
      Collections.sort(getWritableQueue(0));
      
      queueLock.signalAll();
    }
  }
  
  @Override
  public boolean add(T e) {
    if (e == null) {
      return false;
    }
    
    synchronized (queueLock) {
      List<T> queue = getWritableQueue(1);
      int insertionIndex = ListUtils.getInsertionEndIndex(queue, e, randomAccessQueue);
      
      queue.add(insertionIndex, e);
      
      queueLock.notify();
    }
    
    return true;
  }
  
  // must hold queueLock while doing all write operations
  private List<T> getWritableQueue(int expectedSizeIncrease) {
    List<T> newQueueReference = new ArrayList<T>(lastQueueReference.size() + expectedSizeIncrease);
    newQueueReference.addAll(lastQueueReference);
    lastQueueReference = newQueueReference;
      
    return newQueueReference;
  }
  
  private T queuePeek() {
    List<T> queue = lastQueueReference;
    if (queue.isEmpty()) {
      return null;
    } else {
      return queue.get(0);
    }
  }

  @Override
  public void reposition(T e) {
    if (e == null) {
      return;
    }
    
    synchronized (queueLock) {  // must hold queueLock to prevent queue writes in parallel
      boolean found = false;
      // we search backwards because the most common case will be items at the end that need to be repositioned forward
      ListIterator<T> it = lastQueueReference.listIterator(lastQueueReference.size());
      while (it.hasPrevious()) {
        T item = it.previous();
        if (item.equals(e)) {
          it.remove();
          found = true;
          break;
        }
      }
      
      if (! found) {
        throw new IllegalStateException("Could not find item: " + e);
      } else {
        add(e);
      }
    }
  }

  @Override
  public void addLast(T e) {
    if (e == null) {
      throw new NullPointerException();
    }
    
    synchronized (queueLock) {
      getWritableQueue(1).add(e);
    }
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
    T next = queuePeek();
    
    if (next != null && next.getDelay(TimeUnit.MILLISECONDS) > 0) {
      next = null;
    }
    
    return next;
  }

  @Override
  public T poll() {
    T next = queuePeek();
    if (next != null && next.getDelay(TimeUnit.MILLISECONDS) <= 0) {
      // we likely can win, so lets try
      synchronized (queueLock) {
        if ((next = queuePeek()) != null && 
            next.getDelay(TimeUnit.MILLISECONDS) <= 0) {
          return getWritableQueue(0).remove(0);
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
      T next = queuePeek();
      if (next == null) {
        synchronized (queueLock) {
          while ((next = queuePeek()) == null) {
            queueLock.await();
          }
        }
      }
      
      long nextDelay = next.getDelay(TimeUnit.MILLISECONDS);
      if (nextDelay > 0) {
        if (nextDelay > 10) {
          synchronized (queueLock) {
            if (queuePeek() == next) {
              queueLock.await(nextDelay);
            } else {
              continue; // start form the begining
            }
          }
        } else {
          long startTime = ClockWrapper.getAccurateTime();
          long startDelay = nextDelay;
          while ((next = queuePeek()) != null && 
                 (nextDelay = next.getDelay(TimeUnit.MILLISECONDS)) > 0 && 
                 (nextDelay != startDelay || ClockWrapper.getAccurateTime() < startTime + 5)) {
            // spin
          }
          if (nextDelay <= 0) {
            return next;
          } else if (next != null) {
            /* clock is advancing but delay is not, so since this is not
             * a real time delay we just need to wait
             */
            synchronized (queueLock) {
              if (next == queuePeek()) {
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
      if (next == queuePeek()) {
        getWritableQueue(0).remove(0);
      } else {
        next = blockTillAvailable();
        getWritableQueue(0).remove(0);
      }
    }
   
    if (VERBOSE) {
      log.info("Returning next item: " + next + 
                 ", delay is: " + next.getDelay(TimeUnit.MILLISECONDS));
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
    synchronized (queueLock) {
      lastQueueReference = new LinkedList<T>();
    }
  }

  @Override
  public boolean contains(Object o) {
    return lastQueueReference.contains(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return lastQueueReference.containsAll(c);
  }

  @Override
  public boolean isEmpty() {
    return lastQueueReference.isEmpty();
  }

  @Override
  public Iterator<T> iterator() {
    if (! Thread.holdsLock(queueLock)) {
      throw new IllegalStateException("Must have lock in order to get iterator");
    }
    
    return lastQueueReference.iterator();
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
          next = VolatileDynamicDelayQueue.this.peek();
        }
        
        return next != null;
      }
      
      @Override
      public T peek() {
        if (next == null) {
          next = VolatileDynamicDelayQueue.this.peek();
        }
        
        return next;
      }

      @Override
      public T remove() {
        T result;
        if (next != null) {
          result = next;
          getWritableQueue(0).remove(result);
          next = null;
        } else {
          result = VolatileDynamicDelayQueue.this.remove();
        }
        
        return result;
      }
    };
  }

  @Override
  public boolean remove(Object o) {
    synchronized (queueLock) {
      return getWritableQueue(0).remove(o);
    }
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    synchronized (queueLock) {
      return getWritableQueue(0).removeAll(c);
    }
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    synchronized (queueLock) {
      return getWritableQueue(0).retainAll(c);
    }
  }

  @Override
  public int size() {
    return lastQueueReference.size();
  }

  @Override
  public Object[] toArray() {
    return lastQueueReference.toArray();
  }

  @Override
  public <E> E[] toArray(E[] a) {
    return lastQueueReference.toArray(a);
  }
}
