package org.threadly.concurrent.collections;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.threadly.concurrent.collections.ConcurrentArrayList.DataSet;

@SuppressWarnings("javadoc")
public class ConcurrentArrayListDataSetTest {
  // because these objects are immutable, there is no need to create news before each test
  private static final DataSet<Integer> orderedNormal;
  private static final DataSet<Integer> removedFromFront;
  private static final DataSet<Integer> removed2FromFront;
  private static final DataSet<Integer> removedFromEnd;
  private static final DataSet<Integer> removed2FromEnd;
  private static final DataSet<Integer> removedFromBoth;
  
  static {
    Integer[] dataArray = new Integer[]{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    
    orderedNormal = makeDataSet(dataArray, 0, dataArray.length);
    removedFromFront = makeDataSet(dataArray, 1, dataArray.length);
    removed2FromFront = makeDataSet(dataArray, 2, dataArray.length);
    removedFromEnd = makeDataSet(dataArray, 0, dataArray.length - 1);
    removed2FromEnd = makeDataSet(dataArray, 0, dataArray.length - 2);
    removedFromBoth = makeDataSet(dataArray, 1, dataArray.length - 1);
  }
  
  private static <T> DataSet<T> makeDataSet(Object[] dataArray, 
                                            int startPosition, int endPosition) {
    return new DataSet<>(dataArray, startPosition, endPosition, 0, 0);
  }
  
  // little unit test that was made for a very hard to find failure case
  @Test
  public void realWorldRepositionLargeTest() {
    String[] source = new String[] { "24e2bc8d", "21c8dfe6", "ae3865e", "5f8a8ae7", "a574b2", 
                                     "6e905272", "4514f313", "626287d3", "32c3601b", "38daa6a6", 
                                     "1af2f973", "45d6fad7", "24ee5d13", "1d209d56", "4839880a", 
                                     "39126d91", "68d767dc", "15e3d24a", "20030380", "25dad8eb", 
                                     "42d73fb7", "419829a9", "6cd737e3", "25964fe8", "2f581b9f",
                                     "417d7c01", "1558473e", "56ad4264", "8e1dfb1", "2524e205", 
                                     "1872c950", "17feafba", "51c2e8a4", "533790eb", "650b5efb", 
                                     "4d88e490", "5655d1b4", "1c3aacb4", "50206be6", "8bf223", 
                                     "63c5d81c", "51ef4970", "34be8216", "474e8d67", "762589c3", 
                                     "1a779dce", "23194cf5", "520b7ad3", "6facdcb9", "3dcf2ef6", 
                                     "6d14382d", "4a744a4d", "18ba2b6b", "66788a7b", "73cbc5cb", 
                                     "4726cdd1", "449278d5", "6c3b0b1e", "18571615", "7d6ac92e", 
                                     "6dbe2b55", "600f11bc", "7d557ee8", "77fef1a0", "2a97cec", 
                                     "45486b51", "157db660", "705063a5", "2dbe1f3e", "1cf536e8", 
                                     "26e7c832", "3b085e92", "2278e185", "2e1df471", "309b3e5e", 
                                     "201ba640", "773fc437", "7b6b340a", "3b25bbd3", "2a4e37fb", 
                                     "753d556f", "db4268b", "2e4e76b4", "21533b2c", "5f51d6cb", 
                                     "75ecda50", "10f0f6ac", "3bd29ee4", "bda96b", "23bdb02e", 
                                     "7a79ae56", "4aa4ceeb", "a0ccc96", "4e4b9101", "431d00cf", 
                                     "25203875", "665a9c5d", "46cfd22a", "a574b2", "5f8a8ae7"        
    };
    DataSet<String> start = ConcurrentArrayListDataSetTest.<String>makeDataSet(source, 0, source.length);
    DataSet<String> result = start.reposition(92, 2);
    String[] expectedResult = new String[] { "24e2bc8d", "21c8dfe6", "a0ccc96", "ae3865e", "5f8a8ae7", "a574b2", 
                                             "6e905272", "4514f313", "626287d3", "32c3601b", "38daa6a6", 
                                             "1af2f973", "45d6fad7", "24ee5d13", "1d209d56", "4839880a", 
                                             "39126d91", "68d767dc", "15e3d24a", "20030380", "25dad8eb", 
                                             "42d73fb7", "419829a9", "6cd737e3", "25964fe8", "2f581b9f",
                                             "417d7c01", "1558473e", "56ad4264", "8e1dfb1", "2524e205", 
                                             "1872c950", "17feafba", "51c2e8a4", "533790eb", "650b5efb", 
                                             "4d88e490", "5655d1b4", "1c3aacb4", "50206be6", "8bf223", 
                                             "63c5d81c", "51ef4970", "34be8216", "474e8d67", "762589c3", 
                                             "1a779dce", "23194cf5", "520b7ad3", "6facdcb9", "3dcf2ef6", 
                                             "6d14382d", "4a744a4d", "18ba2b6b", "66788a7b", "73cbc5cb", 
                                             "4726cdd1", "449278d5", "6c3b0b1e", "18571615", "7d6ac92e", 
                                             "6dbe2b55", "600f11bc", "7d557ee8", "77fef1a0", "2a97cec", 
                                             "45486b51", "157db660", "705063a5", "2dbe1f3e", "1cf536e8", 
                                             "26e7c832", "3b085e92", "2278e185", "2e1df471", "309b3e5e", 
                                             "201ba640", "773fc437", "7b6b340a", "3b25bbd3", "2a4e37fb", 
                                             "753d556f", "db4268b", "2e4e76b4", "21533b2c", "5f51d6cb", 
                                             "75ecda50", "10f0f6ac", "3bd29ee4", "bda96b", "23bdb02e", 
                                             "7a79ae56", "4aa4ceeb", "4e4b9101", "431d00cf", 
                                             "25203875", "665a9c5d", "46cfd22a", "a574b2", "5f8a8ae7"        
     };
    assertTrue(result.equals(ConcurrentArrayListDataSetTest.<String>makeDataSet(expectedResult, 0, expectedResult.length)));
  }

  // this was another failure that was hard to find
  @Test
  public void realWorldRepositionTest() {
    String[] source = new String[] { "0-0;24ee5d13", "1-1;4839880a", "2-2;45d6fad7", "3-3;68d767dc", 
                                     "4-10;15e3d24a", "5-7;a574b2", "6-8;39126d91", 
                                     "7-9223372036854775807;1af2f973", "8-9223372036854775807;ae3865e", 
                                     "9-9223372036854775807;1d209d56"
    
    };
    DataSet<String> start = ConcurrentArrayListDataSetTest.<String>makeDataSet(source, 0, source.length);
    DataSet<String> result = start.reposition(4, 7);
    String[] expectedResult = new String[] { "0-0;24ee5d13", "1-1;4839880a", "2-2;45d6fad7", "3-3;68d767dc", 
                                             "5-7;a574b2", "6-8;39126d91", "4-10;15e3d24a", 
                                             "7-9223372036854775807;1af2f973", "8-9223372036854775807;ae3865e", 
                                             "9-9223372036854775807;1d209d56"
            
    };
    assertTrue(result.equals(ConcurrentArrayListDataSetTest.<String>makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void equalsBasicTest() {
    assertTrue(orderedNormal.equals(orderedNormal));
    assertFalse(orderedNormal.equals(new Object()));
  }
  
  @Test
  public void equalsEquivelentTest() {
    assertTrue(orderedNormal.equalsEquivelent(orderedNormal));
    assertTrue(removedFromFront.equalsEquivelent(removedFromFront));
    assertTrue(removed2FromFront.equalsEquivelent(removed2FromFront));
    assertTrue(removedFromEnd.equalsEquivelent(removedFromEnd));
    assertTrue(removed2FromEnd.equalsEquivelent(removed2FromEnd));
    assertTrue(removedFromBoth.equalsEquivelent(removedFromBoth));
    
    Integer[] dataArray = new Integer[]{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    
    assertTrue(orderedNormal.equalsEquivelent(makeDataSet(dataArray, 0, dataArray.length)));
    assertTrue(removedFromFront.equalsEquivelent(makeDataSet(dataArray, 1, dataArray.length)));
    assertTrue(removed2FromFront.equalsEquivelent(makeDataSet(dataArray, 2, dataArray.length)));
    assertTrue(removedFromEnd.equalsEquivelent(makeDataSet(dataArray, 0, dataArray.length - 1)));
    assertTrue(removed2FromEnd.equalsEquivelent(makeDataSet(dataArray, 0, dataArray.length - 2)));
    assertTrue(removedFromBoth.equalsEquivelent(makeDataSet(dataArray, 1, dataArray.length - 1)));

    assertTrue(removedFromFront.equalsEquivelent(makeDataSet(dataArray, 1, dataArray.length)
                                                   .addToFront(-1)
                                                   .remove(0)));
    assertTrue(removed2FromFront.equalsEquivelent(makeDataSet(dataArray, 2, dataArray.length)
                                                    .addToFront(-1)
                                                    .remove(0)));
    assertTrue(removedFromEnd.equalsEquivelent(makeDataSet(dataArray, 0, dataArray.length - 1)
                                                 .addToFront(-1)
                                                 .remove(0)));
    assertTrue(removed2FromEnd.equalsEquivelent(makeDataSet(dataArray, 0, dataArray.length - 2)
                                                  .addToFront(-1)
                                                  .remove(0)));
    assertTrue(removedFromBoth.equalsEquivelent(makeDataSet(dataArray, 1, dataArray.length - 1)
                                                  .addToFront(-1)
                                                  .remove(0)));
    
    assertFalse(orderedNormal.equalsEquivelent(removedFromFront));
    assertFalse(orderedNormal.equalsEquivelent(removed2FromFront));
    assertFalse(orderedNormal.equalsEquivelent(removedFromEnd));
    assertFalse(orderedNormal.equalsEquivelent(removed2FromEnd));
    assertFalse(orderedNormal.equalsEquivelent(removedFromBoth));
    
    assertTrue(removedFromFront.equalsEquivelent(orderedNormal.remove(0)));
    assertTrue(removed2FromFront.equalsEquivelent(orderedNormal.remove(0).remove(0)));
    assertTrue(removedFromEnd.equalsEquivelent(orderedNormal.remove(orderedNormal.size - 1)));
    assertTrue(removedFromBoth.equalsEquivelent(orderedNormal.remove(orderedNormal.size - 1).remove(0)));
  }
  
  @Test
  public void equalsExactlyTest() {
    assertTrue(orderedNormal.equalsExactly(orderedNormal));
    assertTrue(removedFromFront.equalsExactly(removedFromFront));
    assertTrue(removed2FromFront.equalsExactly(removed2FromFront));
    assertTrue(removedFromEnd.equalsExactly(removedFromEnd));
    assertTrue(removed2FromEnd.equalsExactly(removed2FromEnd));
    assertTrue(removedFromBoth.equalsExactly(removedFromBoth));
    
    Integer[] dataArray = new Integer[]{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    
    assertTrue(orderedNormal.equalsExactly(makeDataSet(dataArray, 0, dataArray.length)));
    assertTrue(removedFromFront.equalsExactly(makeDataSet(dataArray, 1, dataArray.length)));
    assertTrue(removed2FromFront.equalsExactly(makeDataSet(dataArray, 2, dataArray.length)));
    assertTrue(removedFromEnd.equalsExactly(makeDataSet(dataArray, 0, dataArray.length - 1)));
    assertTrue(removed2FromEnd.equalsExactly(makeDataSet(dataArray, 0, dataArray.length - 2)));
    assertTrue(removedFromBoth.equalsExactly(makeDataSet(dataArray, 1, dataArray.length - 1)));

    assertFalse(removedFromFront.equalsExactly(makeDataSet(dataArray, 1, dataArray.length)
                                                 .addToFront(-1)
                                                 .remove(0)));
    assertFalse(removed2FromFront.equalsExactly(makeDataSet(dataArray, 2, dataArray.length)
                                                  .addToFront(-1)
                                                  .remove(0)));
    assertFalse(removedFromEnd.equalsExactly(makeDataSet(dataArray, 0, dataArray.length - 1)
                                               .addToFront(-1)
                                               .remove(0)));
    assertFalse(removed2FromEnd.equalsExactly(makeDataSet(dataArray, 0, dataArray.length - 2)
                                                .addToFront(-1)
                                                .remove(0)));
    assertFalse(removedFromBoth.equalsExactly(makeDataSet(dataArray, 1, dataArray.length - 1)
                                                .addToFront(-1)
                                                .remove(0)));
    
    assertFalse(orderedNormal.equalsExactly(removedFromFront));
    assertFalse(orderedNormal.equalsExactly(removed2FromFront));
    assertFalse(orderedNormal.equalsExactly(removedFromEnd));
    assertFalse(orderedNormal.equalsExactly(removed2FromEnd));
    assertFalse(orderedNormal.equalsExactly(removedFromBoth));
    
    assertTrue(removedFromFront.equalsExactly(orderedNormal.remove(0)));
    assertTrue(removed2FromFront.equalsExactly(orderedNormal.remove(0).remove(0)));
    assertTrue(removedFromEnd.equalsExactly(orderedNormal.remove(orderedNormal.size - 1)));
    assertTrue(removedFromBoth.equalsExactly(orderedNormal.remove(orderedNormal.size - 1).remove(0)));
  }
  
  @Test
  public void repositionNormalMoveFrontTest() {
    // move front to middle
    DataSet<Integer> result = orderedNormal.reposition(0, 5);
    assertEquals(orderedNormal.size, result.size);
    Integer[] expectedResult = new Integer[]{ 1, 2, 3, 4, 0, 5, 6, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move front to end
    result = orderedNormal.reposition(0, 10);
    expectedResult = new Integer[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionNormalMoveMiddleTest() {
    // move middle to start
    DataSet<Integer> result = orderedNormal.reposition(1, 0);
    assertEquals(orderedNormal.size, result.size);
    Integer[] expectedResult = new Integer[]{ 1, 0, 2, 3, 4, 5, 6, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to middle right
    result = orderedNormal.reposition(1, 5);
    expectedResult = new Integer[]{ 0, 2, 3, 4, 1, 5, 6, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to middle left
    result = orderedNormal.reposition(5, 1);
    expectedResult = new Integer[]{ 0, 5, 1, 2, 3, 4, 6, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to end
    result = orderedNormal.reposition(1, 10);
    expectedResult = new Integer[]{ 0, 2, 3, 4, 5, 6, 7, 8, 9, 1 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionNormalMoveEndTest() {
    // move end to middle
    DataSet<Integer> result = orderedNormal.reposition(9, 5);
    assertEquals(orderedNormal.size, result.size);
    Integer[] expectedResult = new Integer[]{ 0, 1, 2, 3, 4, 9, 5, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move end to start
    result = orderedNormal.reposition(9, 0);
    expectedResult = new Integer[]{ 9, 0, 1, 2, 3, 4, 5, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionFrontRemovedMoveFrontTest() {
    // move front to middle
    DataSet<Integer> result = removedFromFront.reposition(0, 5);
    assertEquals(removedFromFront.size, result.size);
    Integer[] expectedResult = new Integer[]{ 2, 3, 4, 5, 1, 6, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move front to end
    result = removedFromFront.reposition(0, 9);
    expectedResult = new Integer[]{ 2, 3, 4, 5, 6, 7, 8, 9, 1 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionFrontRemovedMoveMiddleTest() {
    // move middle to start
    DataSet<Integer> result = removedFromFront.reposition(1, 0);
    assertEquals(removedFromFront.size, result.size);
    Integer[] expectedResult = new Integer[]{ 2, 1, 3, 4, 5, 6, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to middle right
    result = removedFromFront.reposition(1, 5);
    expectedResult = new Integer[]{ 1, 3, 4, 5, 2, 6, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to middle left
    result = removedFromFront.reposition(5, 1);
    expectedResult = new Integer[]{ 1, 6, 2, 3, 4, 5, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to end
    result = removedFromFront.reposition(1, 9);
    expectedResult = new Integer[]{ 1, 3, 4, 5, 6, 7, 8, 9, 2 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionFrontRemovedMoveEndTest() {
    // move end to middle
    DataSet<Integer> result = removedFromFront.reposition(8, 5);
    assertEquals(removedFromFront.size, result.size);
    Integer[] expectedResult = new Integer[]{ 1, 2, 3, 4, 5, 9, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move end to start
    result = removedFromFront.reposition(8, 0);
    expectedResult = new Integer[]{ 9, 1, 2, 3, 4, 5, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionFront2RemovedMoveFrontTest() {
    // move front to middle
    DataSet<Integer> result = removed2FromFront.reposition(0, 4);
    assertEquals(removed2FromFront.size, result.size);
    Integer[] expectedResult = new Integer[]{ 3, 4, 5, 2, 6, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move front to end
    result = removed2FromFront.reposition(0, 8);
    expectedResult = new Integer[]{ 3, 4, 5, 6, 7, 8, 9, 2 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionFront2RemovedMoveMiddleTest() {
    // move middle to start
    DataSet<Integer> result = removed2FromFront.reposition(1, 0);
    assertEquals(removed2FromFront.size, result.size);
    Integer[] expectedResult = new Integer[]{ 3, 2, 4, 5, 6, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to middle right
    result = removed2FromFront.reposition(1, 5);
    expectedResult = new Integer[]{ 2, 4, 5, 6, 3, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to middle left
    result = removed2FromFront.reposition(5, 1);
    expectedResult = new Integer[]{ 2, 7, 3, 4, 5, 6, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to end
    result = removed2FromFront.reposition(1, 8);
    expectedResult = new Integer[]{ 2, 4, 5, 6, 7, 8, 9, 3 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionFront2RemovedMoveEndTest() {
    // move end to middle
    DataSet<Integer> result = removed2FromFront.reposition(7, 4);
    assertEquals(removed2FromFront.size, result.size);
    Integer[] expectedResult = new Integer[]{ 2, 3, 4, 5, 9, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move end to start
    result = removed2FromFront.reposition(7, 0);
    expectedResult = new Integer[]{ 9, 2, 3, 4, 5, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionRearRemovedMoveFrontTest() {
    // move front to middle
    DataSet<Integer> result = removedFromEnd.reposition(0, 5);
    assertEquals(removedFromEnd.size, result.size);
    Integer[] expectedResult = new Integer[]{ 1, 2, 3, 4, 0, 5, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move front to end
    result = removedFromEnd.reposition(0, 9);
    expectedResult = new Integer[]{ 1, 2, 3, 4, 5, 6, 7, 8, 0 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionRearRemovedMoveMiddleTest() {
    // move middle to start
    DataSet<Integer> result = removedFromEnd.reposition(1, 0);
    assertEquals(removedFromEnd.size, result.size);
    Integer[] expectedResult = new Integer[]{ 1, 0, 2, 3, 4, 5, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to middle right
    result = removedFromEnd.reposition(1, 5);
    expectedResult = new Integer[]{ 0, 2, 3, 4, 1, 5, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to middle left
    result = removedFromEnd.reposition(5, 1);
    expectedResult = new Integer[]{ 0, 5, 1, 2, 3, 4, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to end
    result = removedFromEnd.reposition(1, 9);
    expectedResult = new Integer[]{ 0, 2, 3, 4, 5, 6, 7, 8, 1 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionRearRemovedMoveEndTest() {
    // move end to middle
    DataSet<Integer> result = removedFromEnd.reposition(8, 5);
    assertEquals(removedFromEnd.size, result.size);
    Integer[] expectedResult = new Integer[]{ 0, 1, 2, 3, 4, 8, 5, 6, 7 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move end to start
    result = removedFromEnd.reposition(8, 0);
    expectedResult = new Integer[]{ 8, 0, 1, 2, 3, 4, 5, 6, 7 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionRear2RemovedMoveFrontTest() {
    // move front to middle
    DataSet<Integer> result = removed2FromEnd.reposition(0, 5);
    assertEquals(removed2FromEnd.size, result.size);
    Integer[] expectedResult = new Integer[]{ 1, 2, 3, 4, 0, 5, 6, 7 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move front to end
    result = removed2FromEnd.reposition(0, 8);
    expectedResult = new Integer[]{ 1, 2, 3, 4, 5, 6, 7, 0 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionRear2RemovedMoveMiddleTest() {
    // move middle to start
    DataSet<Integer> result = removed2FromEnd.reposition(1, 0);
    assertEquals(removed2FromEnd.size, result.size);
    Integer[] expectedResult = new Integer[]{ 1, 0, 2, 3, 4, 5, 6, 7 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to middle right
    result = removed2FromEnd.reposition(1, 5);
    expectedResult = new Integer[]{ 0, 2, 3, 4, 1, 5, 6, 7 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to middle left
    result = removed2FromEnd.reposition(5, 1);
    expectedResult = new Integer[]{ 0, 5, 1, 2, 3, 4, 6, 7 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to end
    result = removed2FromEnd.reposition(1, 8);
    expectedResult = new Integer[]{ 0, 2, 3, 4, 5, 6, 7, 1 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionRear2RemovedMoveEndTest() {
    // move end to middle
    DataSet<Integer> result = removed2FromEnd.reposition(7, 5);
    assertEquals(removed2FromEnd.size, result.size);
    Integer[] expectedResult = new Integer[]{ 0, 1, 2, 3, 4, 7, 5, 6 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move end to start
    result = removed2FromEnd.reposition(7, 0);
    expectedResult = new Integer[]{ 7, 0, 1, 2, 3, 4, 5, 6 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionBothRemovedMoveFrontTest() {
    // move front to middle
    DataSet<Integer> result = removedFromBoth.reposition(0, 4);
    assertEquals(removedFromBoth.size, result.size);
    Integer[] expectedResult = new Integer[]{ 2, 3, 4, 1, 5, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move front to end
    result = removedFromBoth.reposition(0, 8);
    expectedResult = new Integer[]{ 2, 3, 4, 5, 6, 7, 8, 1 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionBothRemovedMoveMiddleTest() {
    // move middle to start
    DataSet<Integer> result = removedFromBoth.reposition(1, 0);
    assertEquals(removedFromBoth.size, result.size);
    Integer[] expectedResult = new Integer[]{ 2, 1, 3, 4, 5, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to middle right
    result = removedFromBoth.reposition(1, 4);
    expectedResult = new Integer[]{ 1, 3, 4, 2, 5, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to middle left
    result = removedFromBoth.reposition(4, 1);
    expectedResult = new Integer[]{ 1, 5, 2, 3, 4, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move middle to end
    result = removedFromBoth.reposition(1, 8);
    expectedResult = new Integer[]{ 1, 3, 4, 5, 6, 7, 8, 2 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionBothRemovedMoveEndTest() {
    // move end to middle
    DataSet<Integer> result = removedFromBoth.reposition(7, 3);
    assertEquals(removedFromBoth.size, result.size);
    Integer[] expectedResult = new Integer[]{ 1, 2, 3, 8, 4, 5, 6, 7 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    // move end to start
    result = removedFromBoth.reposition(7, 0);
    expectedResult = new Integer[]{ 8, 1, 2, 3, 4, 5, 6, 7 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void repositionEqualTest() {
    assertTrue(orderedNormal.reposition(0, 0) == orderedNormal);
    assertTrue(removedFromFront.reposition(0, 0) == removedFromFront);
    assertTrue(removed2FromFront.reposition(0, 0) == removed2FromFront);
    assertTrue(removedFromEnd.reposition(0, 0) == removedFromEnd);
    assertTrue(removed2FromEnd.reposition(0, 0) == removed2FromEnd);
    assertTrue(removedFromBoth.reposition(0, 0) == removedFromBoth);
    
    assertTrue(orderedNormal.reposition(1, 1) == orderedNormal);
    assertTrue(removedFromFront.reposition(1, 1) == removedFromFront);
    assertTrue(removed2FromFront.reposition(1, 1) == removed2FromFront);
    assertTrue(removedFromEnd.reposition(1, 1) == removedFromEnd);
    assertTrue(removed2FromEnd.reposition(1, 1) == removed2FromEnd);
    assertTrue(removedFromBoth.reposition(1, 1) == removedFromBoth);
    
    assertTrue(orderedNormal.reposition(orderedNormal.size - 1, orderedNormal.size) == orderedNormal);
    assertTrue(removedFromFront.reposition(removedFromFront.size - 1, removedFromFront.size) == removedFromFront);
    assertTrue(removed2FromFront.reposition(removed2FromFront.size - 1, removed2FromFront.size) == removed2FromFront);
    assertTrue(removedFromEnd.reposition(removedFromEnd.size - 1, removedFromEnd.size) == removedFromEnd);
    assertTrue(removed2FromEnd.reposition(removed2FromEnd.size - 1, removed2FromEnd.size) == removed2FromEnd);
    assertTrue(removedFromBoth.reposition(removedFromBoth.size - 1, removedFromBoth.size) == removedFromBoth);
  }
  
  @Test
  public void optimizedRepositionToEndTest() {
    DataSet<Integer> testSet = new DataSet<>(new Integer[]{ null, null, 0, 1, 2, 3, 4, 5, null, null}, 
                                             2, 8, 2, 2);
    DataSet<Integer> result = testSet.reposition(0, testSet.size);
    
    for (int i = 0; i < result.size; i++) {
      int expected;
      if (i < result.size - 1) {
        expected = i + 1;
      } else {
        expected = 0;
      }
      
      assertTrue(expected == result.get(i));
    }
    
    assertEquals(testSet.dataEndIndex + 1, result.dataEndIndex);
    assertEquals(testSet.dataStartIndex + 1, result.dataStartIndex);
    assertEquals(testSet.size, result.size);
  }
  
  @Test
  public void optimizedRepositionToFrontTest() {
    DataSet<Integer> testSet = new DataSet<>(new Integer[]{ null, null, 0, 1, 2, 3, 4, 5, null, null}, 
                                             2, 8, 2, 2);
    DataSet<Integer> result = testSet.reposition(testSet.size - 1, 0);
    
    for (int i = 0; i < result.size; i++) {
      int expected;
      if (i == 0) {
        expected = 5;
      } else {
        expected = i - 1;
      }
      
      assertTrue(expected == result.get(i));
    }
    
    assertEquals(testSet.dataEndIndex - 1, result.dataEndIndex);
    assertEquals(testSet.dataStartIndex - 1, result.dataStartIndex);
    assertEquals(testSet.size, result.size);
  }
  
  @Test
  public void sizeTest() {
    assertEquals(orderedNormal.dataArray.length, orderedNormal.size);
    assertEquals(orderedNormal.dataArray.length - 1, removedFromFront.size);
    assertEquals(orderedNormal.dataArray.length - 2, removed2FromFront.size);
    assertEquals(orderedNormal.dataArray.length - 1, removedFromEnd.size);
    assertEquals(orderedNormal.dataArray.length - 2, removed2FromEnd.size);
    assertEquals(orderedNormal.dataArray.length - 2, removedFromBoth.size);
  }

  @Test
  public void getNormalTest() {
    assertEquals((Integer)0, orderedNormal.get(0));
    assertEquals((Integer)5, orderedNormal.get(5));
    assertEquals((Integer)9, orderedNormal.get(9));
  }
  
  @Test
  public void getFrontRemovedTest() {
    assertEquals((Integer)1, removedFromFront.get(0));
    assertEquals((Integer)5, removedFromFront.get(4));
    assertEquals((Integer)9, removedFromFront.get(8));
  }
  
  @Test
  public void getFront2RemovedTest() {
    assertEquals((Integer)2, removed2FromFront.get(0));
    assertEquals((Integer)5, removed2FromFront.get(3));
    assertEquals((Integer)9, removed2FromFront.get(7));
  }
  
  @Test
  public void getEndRemovedTest() {
    assertEquals((Integer)0, removedFromEnd.get(0));
    assertEquals((Integer)5, removedFromEnd.get(5));
    assertEquals((Integer)8, removedFromEnd.get(8));
  }
  
  @Test
  public void getEnd2RemovedTest() {
    assertEquals((Integer)0, removed2FromEnd.get(0));
    assertEquals((Integer)5, removed2FromEnd.get(5));
    assertEquals((Integer)7, removed2FromEnd.get(7));
  }
  
  @Test
  public void getBothRemovedTest() {
    assertEquals((Integer)1, removedFromBoth.get(0));
    assertEquals((Integer)6, removedFromBoth.get(5));
    assertEquals((Integer)8, removedFromBoth.get(7));
  }

  @Test
  public void indexOfNormalTest() {
    assertEquals((Integer)(-1), (Integer)orderedNormal.indexOf(-1));
    assertEquals((Integer)0, (Integer)orderedNormal.indexOf(0));
    assertEquals((Integer)5, (Integer)orderedNormal.indexOf(5));
    assertEquals((Integer)9, (Integer)orderedNormal.indexOf(9));
    assertEquals((Integer)(-1), (Integer)orderedNormal.indexOf(10));
  }
  
  @Test
  public void indexOfFrontRemovedTest() {
    assertEquals((Integer)(-1), (Integer)removedFromFront.indexOf(0));
    assertEquals((Integer)0, (Integer)removedFromFront.indexOf(1));
    assertEquals((Integer)4, (Integer)removedFromFront.indexOf(5));
    assertEquals((Integer)8, (Integer)removedFromFront.indexOf(9));
    assertEquals((Integer)(-1), (Integer)removedFromFront.indexOf(10));
  }
  
  @Test
  public void indexOfFront2RemovedTest() {
    assertEquals((Integer)(-1), (Integer)removed2FromFront.indexOf(0));
    assertEquals((Integer)0, (Integer)removed2FromFront.indexOf(2));
    assertEquals((Integer)3, (Integer)removed2FromFront.indexOf(5));
    assertEquals((Integer)7, (Integer)removed2FromFront.indexOf(9));
    assertEquals((Integer)(-1), (Integer)removed2FromFront.indexOf(10));
  }
  
  @Test
  public void indexOfEndRemovedTest() {
    assertEquals((Integer)(-1), (Integer)removedFromEnd.indexOf(-1));
    assertEquals((Integer)0, (Integer)removedFromEnd.indexOf(0));
    assertEquals((Integer)5, (Integer)removedFromEnd.indexOf(5));
    assertEquals((Integer)8, (Integer)removedFromEnd.indexOf(8));
    assertEquals((Integer)(-1), (Integer)removedFromEnd.indexOf(10));
  }
  
  @Test
  public void indexOfEnd2RemovedTest() {
    assertEquals((Integer)(-1), (Integer)removedFromEnd.indexOf(-1));
    assertEquals((Integer)0, (Integer)removedFromEnd.indexOf(0));
    assertEquals((Integer)5, (Integer)removedFromEnd.indexOf(5));
    assertEquals((Integer)7, (Integer)removedFromEnd.indexOf(7));
    assertEquals((Integer)(-1), (Integer)removedFromEnd.indexOf(10));
  }
  
  @Test
  public void indexOfBothRemovedTest() {
    assertEquals((Integer)(-1), (Integer)removedFromBoth.indexOf(-1));
    assertEquals((Integer)0, (Integer)removedFromBoth.indexOf(1));
    assertEquals((Integer)4, (Integer)removedFromBoth.indexOf(5));
    assertEquals((Integer)7, (Integer)removedFromBoth.indexOf(8));
    assertEquals((Integer)(-1), (Integer)removedFromBoth.indexOf(10));
  }
  
  @Test
  public void indexOfDuplicateTest() {
    Integer[] dataArray = new Integer[]{ 0, 1, 1, 5, 5, 8, 8, 9 };
    DataSet<Integer> duplicateSet = makeDataSet(dataArray, 0, dataArray.length);

    assertEquals((Integer)(-1), (Integer)duplicateSet.indexOf(-1));
    assertEquals((Integer)0, (Integer)duplicateSet.indexOf(0));
    assertEquals((Integer)3, (Integer)duplicateSet.indexOf(5));
    assertEquals((Integer)5, (Integer)duplicateSet.indexOf(8));
    assertEquals((Integer)7, (Integer)duplicateSet.indexOf(9));
    assertEquals((Integer)(-1), (Integer)duplicateSet.indexOf(10));
  }

  @Test
  public void lastIndexOfNormalTest() {
    assertEquals((Integer)(-1), (Integer)orderedNormal.lastIndexOf(-1));
    assertEquals((Integer)0, (Integer)orderedNormal.lastIndexOf(0));
    assertEquals((Integer)5, (Integer)orderedNormal.lastIndexOf(5));
    assertEquals((Integer)9, (Integer)orderedNormal.lastIndexOf(9));
    assertEquals((Integer)(-1), (Integer)orderedNormal.lastIndexOf(10));
  }
  
  @Test
  public void lastIndexOfFrontRemovedTest() {
    assertEquals((Integer)(-1), (Integer)removedFromFront.lastIndexOf(0));
    assertEquals((Integer)0, (Integer)removedFromFront.lastIndexOf(1));
    assertEquals((Integer)4, (Integer)removedFromFront.lastIndexOf(5));
    assertEquals((Integer)8, (Integer)removedFromFront.lastIndexOf(9));
    assertEquals((Integer)(-1), (Integer)removedFromFront.lastIndexOf(10));
  }
  
  @Test
  public void lastIndexOfFront2RemovedTest() {
    assertEquals((Integer)(-1), (Integer)removed2FromFront.lastIndexOf(0));
    assertEquals((Integer)0, (Integer)removed2FromFront.lastIndexOf(2));
    assertEquals((Integer)3, (Integer)removed2FromFront.lastIndexOf(5));
    assertEquals((Integer)7, (Integer)removed2FromFront.lastIndexOf(9));
    assertEquals((Integer)(-1), (Integer)removed2FromFront.lastIndexOf(10));
  }
  
  @Test
  public void lastIndexOfEndRemovedTest() {
    assertEquals((Integer)(-1), (Integer)removedFromEnd.lastIndexOf(-1));
    assertEquals((Integer)0, (Integer)removedFromEnd.lastIndexOf(0));
    assertEquals((Integer)5, (Integer)removedFromEnd.lastIndexOf(5));
    assertEquals((Integer)8, (Integer)removedFromEnd.lastIndexOf(8));
    assertEquals((Integer)(-1), (Integer)removedFromEnd.lastIndexOf(10));
  }
  
  @Test
  public void lastIndexOfEnd2RemovedTest() {
    assertEquals((Integer)(-1), (Integer)removedFromEnd.lastIndexOf(-1));
    assertEquals((Integer)0, (Integer)removedFromEnd.lastIndexOf(0));
    assertEquals((Integer)5, (Integer)removedFromEnd.lastIndexOf(5));
    assertEquals((Integer)7, (Integer)removedFromEnd.lastIndexOf(7));
    assertEquals((Integer)(-1), (Integer)removedFromEnd.lastIndexOf(10));
  }
  
  @Test
  public void lastIndexOfBothRemovedTest() {
    assertEquals((Integer)(-1), (Integer)removedFromBoth.lastIndexOf(-1));
    assertEquals((Integer)0, (Integer)removedFromBoth.lastIndexOf(1));
    assertEquals((Integer)4, (Integer)removedFromBoth.lastIndexOf(5));
    assertEquals((Integer)7, (Integer)removedFromBoth.lastIndexOf(8));
    assertEquals((Integer)(-1), (Integer)removedFromBoth.lastIndexOf(10));
  }
  
  @Test
  public void lastIndexOfDuplicateTest() {
    Integer[] dataArray = new Integer[]{ 0, 1, 1, 5, 5, 8, 8, 9 };
    DataSet<Integer> duplicateSet = makeDataSet(dataArray, 0, dataArray.length);

    assertEquals((Integer)(-1), (Integer)duplicateSet.lastIndexOf(-1));
    assertEquals((Integer)0, (Integer)duplicateSet.lastIndexOf(0));
    assertEquals((Integer)4, (Integer)duplicateSet.lastIndexOf(5));
    assertEquals((Integer)6, (Integer)duplicateSet.lastIndexOf(8));
    assertEquals((Integer)7, (Integer)duplicateSet.lastIndexOf(9));
    assertEquals((Integer)(-1), (Integer)duplicateSet.lastIndexOf(10));
  }
  
  @Test
  public void setNormalTest() {
    DataSet<Integer> result = orderedNormal.set(10, 100).set(5,0).set(0, 10);
    Integer[] expectedResult = new Integer[]{ 10, 1, 2, 3, 4, 0, 6, 7, 8, 9, 100 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void setFrontRemovedTest() {
    DataSet<Integer> result = removedFromFront.set(9, 100).set(5,0).set(0, 10);
    Integer[] expectedResult = new Integer[]{ 10, 2, 3, 4, 5, 0, 7, 8, 9, 100 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void setFront2RemovedTest() {
    DataSet<Integer> result = removed2FromFront.set(8, 100).set(5,0).set(0, 10);
    Integer[] expectedResult = new Integer[]{ 10, 3, 4, 5, 6, 0, 8, 9, 100 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void setEndRemovedTest() {
    DataSet<Integer> result = removedFromEnd.set(9, 100).set(5,0).set(0, 10);
    Integer[] expectedResult = new Integer[]{ 10, 1, 2, 3, 4, 0, 6, 7, 8, 100 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void setEnd2RemovedTest() {
    DataSet<Integer> result = removed2FromEnd.set(8, 100).set(5,0).set(0, 10);
    Integer[] expectedResult = new Integer[]{ 10, 1, 2, 3, 4, 0, 6, 7, 100 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void setBothRemovedTest() {
    DataSet<Integer> result = removedFromBoth.set(8, 100).set(5,0).set(0, 10);
    Integer[] expectedResult = new Integer[]{ 10, 2, 3, 4, 5, 0, 7, 8, 100 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addNormalTest() {
    DataSet<Integer> result = orderedNormal.addToEnd(100);
    Integer[] expectedResult = new Integer[]{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 100 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addFrontRemovedTest() {
    DataSet<Integer> result = removedFromFront.addToEnd(100);
    Integer[] expectedResult = new Integer[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 100 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addFront2RemovedTest() {
    DataSet<Integer> result = removed2FromFront.addToEnd(100);
    Integer[] expectedResult = new Integer[]{ 2, 3, 4, 5, 6, 7, 8, 9, 100 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addEndRemovedTest() {
    DataSet<Integer> result = removedFromEnd.addToEnd(100);
    Integer[] expectedResult = new Integer[]{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 100 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addEnd2RemovedTest() {
    DataSet<Integer> result = removed2FromEnd.addToEnd(100);
    Integer[] expectedResult = new Integer[]{ 0, 1, 2, 3, 4, 5, 6, 7, 100 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addBothRemovedTest() {
    DataSet<Integer> result = removedFromBoth.addToEnd(100);
    Integer[] expectedResult = new Integer[]{ 1, 2, 3, 4, 5, 6, 7, 8, 100 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addIndexNormalTest() {
    DataSet<Integer> result = orderedNormal.add(5, 100).add(0, 200);  // perform two adds, one at the start, one at the middle
    Integer[] expectedResult = new Integer[]{ 200, 0, 1, 2, 3, 4, 100, 5, 6, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addIndexFrontRemovedTest() {
    DataSet<Integer> result = removedFromFront.add(5, 100).add(0, 200);  // perform two adds, one at the start, one at the middle
    Integer[] expectedResult = new Integer[]{ 200, 1, 2, 3, 4, 5, 100, 6, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addIndexFront2RemovedTest() {
    DataSet<Integer> result = removed2FromFront.add(5, 100).add(0, 200);  // perform two adds, one at the start, one at the middle
    Integer[] expectedResult = new Integer[]{ 200, 2, 3, 4, 5, 6, 100, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addIndexEndRemovedTest() {
    DataSet<Integer> result = removedFromEnd.add(5, 100).add(0, 200);  // perform two adds, one at the start, one at the middle
    Integer[] expectedResult = new Integer[]{ 200, 0, 1, 2, 3, 4, 100, 5, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addIndexEnd2RemovedTest() {
    DataSet<Integer> result = removed2FromEnd.add(5, 100).add(0, 200);  // perform two adds, one at the start, one at the middle
    Integer[] expectedResult = new Integer[]{ 200, 0, 1, 2, 3, 4, 100, 5, 6, 7 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addIndexBothRemovedTest() {
    DataSet<Integer> result = removedFromBoth.add(5, 100).add(0, 200);  // perform two adds, one at the start, one at the middle
    Integer[] expectedResult = new Integer[]{ 200, 1, 2, 3, 4, 5, 100, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addAllNormalTest() {
    List<Integer> toAddList = new ArrayList<>(5);
    toAddList.add(100);
    toAddList.add(200);
    toAddList.add(300);
    toAddList.add(400);
    toAddList.add(500);
    DataSet<Integer> result = orderedNormal.addAll(toAddList);
    Integer[] expectedResult = new Integer[]{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 100, 200, 300, 400, 500 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addAllFrontRemovedTest() {
    List<Integer> toAddList = new ArrayList<>(5);
    toAddList.add(100);
    toAddList.add(200);
    toAddList.add(300);
    toAddList.add(400);
    toAddList.add(500);
    DataSet<Integer> result = removedFromFront.addAll(toAddList);
    Integer[] expectedResult = new Integer[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 100, 200, 300, 400, 500 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addAllFront2RemovedTest() {
    List<Integer> toAddList = new ArrayList<>(5);
    toAddList.add(100);
    toAddList.add(200);
    toAddList.add(300);
    toAddList.add(400);
    toAddList.add(500);
    DataSet<Integer> result = removed2FromFront.addAll(toAddList);
    Integer[] expectedResult = new Integer[]{ 2, 3, 4, 5, 6, 7, 8, 9, 100, 200, 300, 400, 500 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addAllEndRemovedTest() {
    List<Integer> toAddList = new ArrayList<>(5);
    toAddList.add(100);
    toAddList.add(200);
    toAddList.add(300);
    toAddList.add(400);
    toAddList.add(500);
    DataSet<Integer> result = removedFromEnd.addAll(toAddList);
    Integer[] expectedResult = new Integer[]{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 100, 200, 300, 400, 500 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addAllEnd2RemovedTest() {
    List<Integer> toAddList = new ArrayList<>(5);
    toAddList.add(100);
    toAddList.add(200);
    toAddList.add(300);
    toAddList.add(400);
    toAddList.add(500);
    DataSet<Integer> result = removed2FromEnd.addAll(toAddList);
    Integer[] expectedResult = new Integer[]{ 0, 1, 2, 3, 4, 5, 6, 7, 100, 200, 300, 400, 500 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addAllBothRemovedTest() {
    List<Integer> toAddList = new ArrayList<>(5);
    toAddList.add(100);
    toAddList.add(200);
    toAddList.add(300);
    toAddList.add(400);
    toAddList.add(500);
    DataSet<Integer> result = removedFromBoth.addAll(toAddList);
    Integer[] expectedResult = new Integer[]{ 1, 2, 3, 4, 5, 6, 7, 8, 100, 200, 300, 400, 500 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addAllIndexNormalTest() {
    List<Integer> toAddList = new ArrayList<>(5);
    toAddList.add(100);
    toAddList.add(200);
    toAddList.add(300);
    toAddList.add(400);
    toAddList.add(500);
    DataSet<Integer> result = orderedNormal.addAll(5, toAddList).addAll(0, toAddList);  // perform two adds, one at the start, one at the middle
    Integer[] expectedResult = new Integer[]{ 100, 200, 300, 400, 500, 0, 1, 2, 3, 4, 100, 200, 300, 400, 500, 5, 6, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addAllIndexFrontRemovedTest() {
    List<Integer> toAddList = new ArrayList<>(5);
    toAddList.add(100);
    toAddList.add(200);
    toAddList.add(300);
    toAddList.add(400);
    toAddList.add(500);
    DataSet<Integer> result = removedFromFront.addAll(5, toAddList).addAll(0, toAddList);  // perform two adds, one at the start, one at the middle
    Integer[] expectedResult = new Integer[]{ 100, 200, 300, 400, 500, 1, 2, 3, 4, 5, 100, 200, 300, 400, 500, 6, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addAllIndexFront2RemovedTest() {
    List<Integer> toAddList = new ArrayList<>(5);
    toAddList.add(100);
    toAddList.add(200);
    toAddList.add(300);
    toAddList.add(400);
    toAddList.add(500);
    DataSet<Integer> result = removed2FromFront.addAll(5, toAddList).addAll(0, toAddList);  // perform two adds, one at the start, one at the middle
    Integer[] expectedResult = new Integer[]{ 100, 200, 300, 400, 500, 2, 3, 4, 5, 6, 100, 200, 300, 400, 500, 7, 8, 9 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addAllIndexEndRemovedTest() {
    List<Integer> toAddList = new ArrayList<>(5);
    toAddList.add(100);
    toAddList.add(200);
    toAddList.add(300);
    toAddList.add(400);
    toAddList.add(500);
    DataSet<Integer> result = removedFromEnd.addAll(5, toAddList).addAll(0, toAddList);  // perform two adds, one at the start, one at the middle
    Integer[] expectedResult = new Integer[]{ 100, 200, 300, 400, 500, 0, 1, 2, 3, 4, 100, 200, 300, 400, 500, 5, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addAllIndexEnd2RemovedTest() {
    List<Integer> toAddList = new ArrayList<>(5);
    toAddList.add(100);
    toAddList.add(200);
    toAddList.add(300);
    toAddList.add(400);
    toAddList.add(500);
    DataSet<Integer> result = removed2FromEnd.addAll(5, toAddList).addAll(0, toAddList);  // perform two adds, one at the start, one at the middle
    Integer[] expectedResult = new Integer[]{ 100, 200, 300, 400, 500, 0, 1, 2, 3, 4, 100, 200, 300, 400, 500, 5, 6, 7 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addAllIndexBothRemovedTest() {
    List<Integer> toAddList = new ArrayList<>(5);
    toAddList.add(100);
    toAddList.add(200);
    toAddList.add(300);
    toAddList.add(400);
    toAddList.add(500);
    DataSet<Integer> result = removedFromBoth.addAll(5, toAddList).addAll(0, toAddList);  // perform two adds, one at the start, one at the middle
    Integer[] expectedResult = new Integer[]{ 100, 200, 300, 400, 500, 1, 2, 3, 4, 5, 100, 200, 300, 400, 500, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void addAllFrontSpaceTest() {
    List<Integer> toAddList = new ArrayList<>(5);
    toAddList.add(100);
    toAddList.add(200);
    toAddList.add(300);
    toAddList.add(400);
    toAddList.add(500);
    Integer[] emptyArray = new Integer[toAddList.size()];
    DataSet<Integer> result = makeDataSet(emptyArray, toAddList.size(), toAddList.size());
    result = result.addAll(0, toAddList);
    Integer[] expectedResult = new Integer[]{ 100, 200, 300, 400, 500};
    assertTrue(result.equalsExactly(makeDataSet(expectedResult, 0, expectedResult.length)));
    assertTrue(result.dataArray == emptyArray);
  }
  
  @Test
  public void addAllEndSpaceTest() {
    List<Integer> toAddList = new ArrayList<>(5);
    toAddList.add(100);
    toAddList.add(200);
    toAddList.add(300);
    toAddList.add(400);
    toAddList.add(500);
    Integer[] emptyArray = new Integer[toAddList.size() + 1];
    emptyArray[0] = -1;
    DataSet<Integer> result = makeDataSet(emptyArray, 0, 1);
    result = result.addAll(1, toAddList);
    Integer[] expectedResult = new Integer[]{ -1, 100, 200, 300, 400, 500};
    assertTrue(result.equalsExactly(makeDataSet(expectedResult, 0, expectedResult.length)));
    assertTrue(result.dataArray == emptyArray);
  }
  
  @Test
  public void removeIndexNormalTest() {
    DataSet<Integer> result = orderedNormal.remove(9).remove(5).remove(0);
    Integer[] expectedResult = new Integer[]{ 1, 2, 3, 4, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void removeIndexFrontRemovedTest() {
    DataSet<Integer> result = removedFromFront.remove(8).remove(5).remove(0);
    Integer[] expectedResult = new Integer[]{ 2, 3, 4, 5, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void removeIndexFront2RemovedTest() {
    DataSet<Integer> result = removed2FromFront.remove(7).remove(5).remove(0);
    Integer[] expectedResult = new Integer[]{ 3, 4, 5, 6, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void removeIndexEndRemovedTest() {
    DataSet<Integer> result = removedFromEnd.remove(8).remove(5).remove(0);
    Integer[] expectedResult = new Integer[]{ 1, 2, 3, 4, 6, 7 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void removeIndexEnd2RemovedTest() {
    DataSet<Integer> result = removed2FromEnd.remove(7).remove(5).remove(0);
    Integer[] expectedResult = new Integer[]{ 1, 2, 3, 4, 6 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void removeIndexBothRemovedTest() {
    DataSet<Integer> result = removedFromBoth.remove(7).remove(5).remove(0);
    Integer[] expectedResult = new Integer[]{ 2, 3, 4, 5, 7 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
  }
  
  @Test
  public void removeAllNormalTest() {
    List<Integer> toRemove = new ArrayList<>(4);
    toRemove.add(0);  // front
    toRemove.add(5);  // middle
    toRemove.add(9);  // end
    toRemove.add(100);  // not found
    DataSet<Integer> result = orderedNormal.removeAll(toRemove);
    Integer[] expectedResult = new Integer[]{ 1, 2, 3, 4, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    assertEquals(orderedNormal.size - toRemove.size() + 1, result.size);
  }
  
  @Test
  public void removeAllFrontRemovedTest() {
    List<Integer> toRemove = new ArrayList<>(4);
    toRemove.add(1);  // front
    toRemove.add(5);  // middle
    toRemove.add(9);  // end
    toRemove.add(100);  // not found
    DataSet<Integer> result = removedFromFront.removeAll(toRemove);
    Integer[] expectedResult = new Integer[]{ 2, 3, 4, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    assertEquals(removedFromFront.size - toRemove.size() + 1, result.size);
  }
  
  @Test
  public void removeAllFront2RemovedTest() {
    List<Integer> toRemove = new ArrayList<>(4);
    toRemove.add(2);  // front
    toRemove.add(5);  // middle
    toRemove.add(9);  // end
    toRemove.add(100);  // not found
    DataSet<Integer> result = removed2FromFront.removeAll(toRemove);
    Integer[] expectedResult = new Integer[]{ 3, 4, 6, 7, 8 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    assertEquals(removed2FromFront.size - toRemove.size() + 1, result.size);
  }
  
  @Test
  public void removeAllEndRemovedTest() {
    List<Integer> toRemove = new ArrayList<>(4);
    toRemove.add(0);  // front
    toRemove.add(5);  // middle
    toRemove.add(8);  // end
    toRemove.add(100);  // not found
    DataSet<Integer> result = removedFromEnd.removeAll(toRemove);
    Integer[] expectedResult = new Integer[]{ 1, 2, 3, 4, 6, 7 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    assertEquals(removedFromEnd.size - toRemove.size() + 1, result.size);
  }
  
  @Test
  public void removeAllEnd2RemovedTest() {
    List<Integer> toRemove = new ArrayList<>(4);
    toRemove.add(0);  // front
    toRemove.add(5);  // middle
    toRemove.add(7);  // end
    toRemove.add(100);  // not found
    DataSet<Integer> result = removed2FromEnd.removeAll(toRemove);
    Integer[] expectedResult = new Integer[]{ 1, 2, 3, 4, 6 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    assertEquals(removed2FromEnd.size - toRemove.size() + 1, result.size);
  }
  
  @Test
  public void removeAllBothRemovedTest() {
    List<Integer> toRemove = new ArrayList<>(4);
    toRemove.add(1);  // front
    toRemove.add(5);  // middle
    toRemove.add(8);  // end
    toRemove.add(100);  // not found
    DataSet<Integer> result = removedFromBoth.removeAll(toRemove);
    Integer[] expectedResult = new Integer[]{ 2, 3, 4, 6, 7 };
    assertTrue(result.equals(makeDataSet(expectedResult, 0, expectedResult.length)));
    assertEquals(removedFromBoth.size - toRemove.size() + 1, result.size);
  }

  //System.out.println(result); // S.0 .1 .2 .3 .4 .5 .6 .7 .8 .9E
}
