package org.threadly.util;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.Writer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;

@SuppressWarnings("javadoc")
public class StringBuilderWriterTest extends ThreadlyTester {
  private StringBuilder sb;
  private StringBuilderWriter sbw;
  
  @BeforeEach
  public void setup() {
    sb = new StringBuilder();
    sbw = new StringBuilderWriter(sb);
  }
  
  @AfterEach
  public void cleanup() {
    sb = null;
    sbw.close();
    sbw = null;
  }
  
  @SuppressWarnings({ "resource", "unused" })
  @Test
  public void constructorFail() {
      assertThrows(IllegalArgumentException.class, () -> {
      new StringBuilderWriter(null);
      });
  }
  
  @SuppressWarnings("resource")
  @Test
  public void appendCharTest() throws IOException {
    int start = 0;
    int end = 10;
    
    Writer w = sbw;
    for (int i = start; i <= end; i++) {
      w = w.append((char)i);
    }
    
    for (int i = start; i <= end; i++) {
      assertEquals((char)i, sb.charAt(i - start));
    }
  }
  
  @Test
  @SuppressWarnings("resource")
  public void appendCharSquenceTest() {
    String testStr = "The quick brown fox jumped over the lazy dog!";
    
    sbw.append(testStr);
    
    assertEquals(testStr, sb.toString());
    
    sbw.append(testStr);
    
    assertEquals(testStr + testStr, sb.toString());
  }
  
  @Test
  @SuppressWarnings("resource")
  public void appendCharSquenceRangeTest() {
    int rangeStart = 0;
    int rangeEnd = 10;
    String testStr = "The quick brown fox jumped over the lazy dog!";
    
    sbw.append(testStr, 0, rangeEnd);
    
    assertEquals(testStr.subSequence(rangeStart, rangeEnd), sb.toString());
  }
  
  @Test
  public void writeTest() {
    int start = 0;
    int end = 10;
    
    for (int i = start; i <= end; i++) {
      sbw.write((char)i);
    }
    
    for (int i = start; i <= end; i++) {
      assertEquals((char)i, sb.charAt(i - start));
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
      assertEquals((char)i, sb.charAt(i - start));
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
    
    assertEquals(compareStr, sb.toString());
  }
  
  @Test
  public void lengthTest() {
    assertEquals(sb.length(), sbw.length());
    sb.append("foo");
    assertEquals(sb.length(), sbw.length());
  }
  
  @Test
  public void charAtTest() {
    sb.append("foo");
    for (int i = 0; i < sb.length(); i++) {
      assertEquals(sb.charAt(i), sbw.charAt(i));
    }
  }
  
  @Test
  public void subSequenceTest() {
    sb.append("0123456789");
    
    for (int i = 2; i < sb.length(); i++) {
      assertEquals(sb.subSequence(1, i), sbw.subSequence(1, i));
    }
  }
}
