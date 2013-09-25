package org.threadly.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.Writer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("javadoc")
public class StringBufferWriterTest {
  private StringBuffer sb;
  private StringBufferWriter sbw;
  
  @Before
  public void setup() {
    sb = new StringBuffer();
    sbw = new StringBufferWriter(sb);
  }
  
  @After
  public void tearDown() {
    sb = null;
    sbw.close();
    sbw = null;
  }
  
  @Test
  public void appendCharTest() throws IOException {
    int start = 0;
    int end = 10;
    
    Writer w = sbw;
    for (int i = start; i <= end; i++) {
      w = w.append((char)i);
    }
    
    for (int i = start; i <= end; i++) {
      assertEquals(sb.charAt(i - start), (char)i);
    }
  }
  
  @Test
  public void appendCharSquenceTest() {
    String testStr = "The quick brown fox jumped over the lazy dog!";
    
    sbw.append(testStr);
    
    assertEquals(sb.toString(), testStr);
    
    sbw.append(testStr);
    
    assertEquals(sb.toString(), testStr + testStr);
  }
  
  @Test
  public void appendCharSquenceRangeTest() {
    int rangeStart = 0;
    int rangeEnd = 10;
    String testStr = "The quick brown fox jumped over the lazy dog!";
    
    sbw.append(testStr, 0, rangeEnd);
    
    assertEquals(sb.toString(), testStr.subSequence(rangeStart, rangeEnd));
  }
  
  @Test
  public void writeTest() {
    int start = 0;
    int end = 10;
    
    for (int i = start; i <= end; i++) {
      sbw.write((char)i);
    }
    
    for (int i = start; i <= end; i++) {
      assertEquals(sb.charAt(i - start), (char)i);
    }
  }
  
  @Test
  public void writeCharArrayTest() {
    int start = 0;
    int end = 10;
    
    char[] data = new char[end - start + 1];
    
    for (int i = start; i <= end; i++) {
      data[i - start] = (char)i;
    }
    
    sbw.write(data);
    
    for (int i = start; i <= end; i++) {
      assertEquals(sb.charAt(i - start), (char)i);
    }
  }
  
  @Test
  public void writeCharArrayRangeTest() {
    int rangeStart = 0;
    int rangeEnd = 25;
    int start = 0;
    int end = 100;
    
    char[] data = new char[end - start + 1];
    
    for (int i = start; i <= end; i++) {
      data[i - start] = (char)i;
    }
    
    sbw.write(data, rangeStart, rangeEnd);
    
    String compareStr = new String(data).substring(rangeStart, rangeEnd);
    
    assertEquals(sb.toString(), compareStr);
  }
}
