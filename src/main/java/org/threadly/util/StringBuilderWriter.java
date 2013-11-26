package org.threadly.util;

import java.io.Writer;

/**
 * <p>Class to wrap a {@link StringBuilder} to implement the {@link Writer} interface.</p>
 * 
 * @author jent - Mike Jensen
 */
public class StringBuilderWriter extends Writer implements CharSequence {
  private final StringBuilder sb;
  
  /**
   * Constructs a new writer with the provided {@link StringBuilder} to write to.
   * 
   * @param sb string builder to write to, can not be null
   */
  public StringBuilderWriter(StringBuilder sb) {
    if (sb == null) {
      throw new IllegalArgumentException("Must provide string builder to write to");
    }
    
    this.sb = sb;
  }
  
  @Override
  public Writer append(char c) {
    sb.append(c);
    
    return this;
  }
  
  @Override
  public Writer append(CharSequence cSeq) {
    sb.append(cSeq);
    
    return this;
  }
  
  @Override
  public Writer append(CharSequence cSeq, int start, int end) {
    sb.append(cSeq, start, end);
    
    return this;
  }

  @Override
  public void write(int c) {
    sb.append((char)c);
  }

  @Override
  public void write(char[] cbuf) {
    sb.append(cbuf);
  }

  @Override
  public void write(char[] cbuf, int offset, int len) {
    sb.append(cbuf, offset, len);
  }

  @Override
  public void flush() {
    // ignored
  }

  @Override
  public void close() {
    // ignored
  }

  @Override
  public int length() {
    return sb.length();
  }

  @Override
  public char charAt(int index) {
    return sb.charAt(index);
  }

  @Override
  public CharSequence subSequence(int start, int end) {
    return sb.subSequence(start, end);
  }
}
