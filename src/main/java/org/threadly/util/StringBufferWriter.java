package org.threadly.util;

import java.io.Writer;

/**
 * Class to wrap a {@link StringBuffer} to implement the {@link Writer} interface.
 * 
 * @since 1.0.0
 */
public class StringBufferWriter extends Writer implements CharSequence {
  protected final StringBuffer sb;
  
  /**
   * Constructs a new writer with the provided {@link StringBuffer} to write to.
   * 
   * @param stringBuffer string buffer to write to, can not be {@code null}
   */
  public StringBufferWriter(StringBuffer stringBuffer) {
    ArgumentVerifier.assertNotNull(stringBuffer, "stringBuffer");
    
    this.sb = stringBuffer;
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
