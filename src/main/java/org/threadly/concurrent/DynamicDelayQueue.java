package org.threadly.concurrent;

import java.util.Queue;
import java.util.concurrent.Delayed;

public interface DynamicDelayQueue<T extends Delayed> extends Queue<T> {
  public Object getLock();
  
  public void sortQueue();
  
  public void reposition(T e);
  
  public void addLast(T e);
  
  public T take() throws InterruptedException;
  
  public ConsumerIterator<T> consumeIterator() throws InterruptedException;
  
  public interface ConsumerIterator<E> {
    public boolean hasNext();
    
    public E peek();
    
    public E remove();
  }
}