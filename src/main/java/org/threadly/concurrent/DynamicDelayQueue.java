package org.threadly.concurrent;

import java.util.Queue;
import java.util.concurrent.Delayed;

public interface DynamicDelayQueue<T extends Delayed> extends Queue<T> {
  
  public abstract Object getLock();
  
  public abstract void sortQueue();
  
  public abstract void reposition(T e);
  
  public abstract void addLast(T e);
  
  public abstract T take() throws InterruptedException;
  
  public abstract ConsumerIterator<T> consumeIterator() throws InterruptedException;
  
  public interface ConsumerIterator<E> {
    public boolean hasNext();
    
    public E peek();
    
    public E remove();
  }
}