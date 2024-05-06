/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueViewVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.impl.UnionListViewWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.holders.DurationHolder;
import org.apache.arrow.vector.holders.TimeStampMilliTZHolder;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.DataSizeRoundingUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestListViewVector {

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @AfterEach
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testBasicListViewVector() {
    try (ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator)) {
      UnionListViewWriter listViewWriter = listViewVector.getWriter();

      /* allocate memory */
      listViewWriter.allocate();

      /* write the first list at index 0 */
      listViewWriter.setPosition(0);
      listViewWriter.startList();

      listViewWriter.bigInt().writeBigInt(12);
      listViewWriter.bigInt().writeBigInt(-7);
      listViewWriter.bigInt().writeBigInt(25);
      listViewWriter.endList();

      /* the second list at index 2 is null (we are not setting any)*/

      /* write the third list at index 2 */
      listViewWriter.setPosition(2);
      listViewWriter.startList();

      listViewWriter.bigInt().writeBigInt(0);
      listViewWriter.bigInt().writeBigInt(-127);
      listViewWriter.bigInt().writeBigInt(127);
      listViewWriter.bigInt().writeBigInt(50);
      listViewWriter.endList();

      /* write the fourth list at index 3 (empty list) */
      listViewWriter.setPosition(3);
      listViewWriter.startList();
      listViewWriter.endList();

      /* write the fifth list at index 4 */
      listViewWriter.setPosition(4);
      listViewWriter.startList();
      listViewWriter.bigInt().writeBigInt(1);
      listViewWriter.bigInt().writeBigInt(2);
      listViewWriter.bigInt().writeBigInt(3);
      listViewWriter.bigInt().writeBigInt(4);
      listViewWriter.endList();

      assertEquals(4, listViewVector.getLastSet());

      listViewVector.setValueCount(5);
      // check value count
      assertEquals(5, listViewVector.getValueCount());

      /* get vector at index 0 -- the value is a BigIntVector*/
      final ArrowBuf offSetBuffer = listViewVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();
      final FieldVector dataVec = listViewVector.getDataVector();

      // check offset buffer
      assertEquals(0, offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(2 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(3 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(4 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(3, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(4, sizeBuffer.getInt(2 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(3 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(4, sizeBuffer.getInt(4 * BaseRepeatedValueViewVector.SIZE_WIDTH));

      // check data vector
      assertEquals(12, ((BigIntVector) dataVec).get(0));
      assertEquals(-7, ((BigIntVector) dataVec).get(1));
      assertEquals(25, ((BigIntVector) dataVec).get(2));
      assertEquals(0, ((BigIntVector) dataVec).get(3));
      assertEquals(-127, ((BigIntVector) dataVec).get(4));
      assertEquals(127, ((BigIntVector) dataVec).get(5));
      assertEquals(50, ((BigIntVector) dataVec).get(6));
      assertEquals(1, ((BigIntVector) dataVec).get(7));
      assertEquals(2, ((BigIntVector) dataVec).get(8));
      assertEquals(3, ((BigIntVector) dataVec).get(9));
      assertEquals(4, ((BigIntVector) dataVec).get(10));
    }
  }

  @Test
  public void testImplicitNullVectors() {
    try (ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator)) {
      UnionListViewWriter listViewWriter = listViewVector.getWriter();
      /* allocate memory */
      listViewWriter.allocate();

      final ArrowBuf offSetBuffer = listViewVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();

      /* write the first list at index 0 */
      listViewWriter.setPosition(0);
      listViewWriter.startList();

      listViewWriter.bigInt().writeBigInt(12);
      listViewWriter.bigInt().writeBigInt(-7);
      listViewWriter.bigInt().writeBigInt(25);
      listViewWriter.endList();

      int offSet0 = offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH);
      int size0 = sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH);
      int lastSet0 = listViewVector.getLastSet();

      // after the first list is written,
      // the initial offset must be 0,
      // the size must be 3 (as there are 3 elements in the array),
      // the lastSet must be 0 since, the first list is written at index 0.

      assertEquals(0, offSet0);
      assertEquals(3, size0);
      assertEquals(0, lastSet0);

      listViewWriter.setPosition(5);
      listViewWriter.startList();

      // writing the 6th list at index 5,
      // and the list items from index 1 through 4 are not populated.
      // but since there is a gap between the 0th and 5th list, in terms
      // of buffer allocation, the offset and size buffers must be updated
      // to reflect the implicit null vectors.

      for (int i = 1; i < 5; i++) {
        int offSet = offSetBuffer.getInt(i * BaseRepeatedValueViewVector.OFFSET_WIDTH);
        int size = sizeBuffer.getInt(i * BaseRepeatedValueViewVector.SIZE_WIDTH);
        int lastSet = listViewVector.getLastSet();
        // Since the list is not written, the offset and size must equal to child vector's size
        // i.e., 3, and size should be 0 as the list is not written.
        // And the last set value is the value currently being written, which is 5.
        assertEquals(3, offSet);
        assertEquals(0, size);
        assertEquals(5, lastSet);
      }

      listViewWriter.bigInt().writeBigInt(12);
      listViewWriter.bigInt().writeBigInt(25);
      listViewWriter.endList();

      int offSet5 = offSetBuffer.getInt(5 * BaseRepeatedValueViewVector.OFFSET_WIDTH);
      int size5 = sizeBuffer.getInt(5 * BaseRepeatedValueViewVector.SIZE_WIDTH);
      int lastSet5 = listViewVector.getLastSet();

      assertEquals(3, offSet5);
      assertEquals(2, size5);
      assertEquals(5, lastSet5);

      listViewWriter.setPosition(10);
      listViewWriter.startList();

      // writing the 11th list at index 10,
      // and the list items from index 6 through 10 are not populated.
      // but since there is a gap between the 5th and 11th list, in terms
      // of buffer allocation, the offset and size buffers must be updated
      // to reflect the implicit null vectors.
      for (int i = 6; i < 10; i++) {
        int offSet = offSetBuffer.getInt(i * BaseRepeatedValueViewVector.OFFSET_WIDTH);
        int size = sizeBuffer.getInt(i * BaseRepeatedValueViewVector.SIZE_WIDTH);
        int lastSet = listViewVector.getLastSet();
        // Since the list is not written, the offset and size must equal to child vector's size
        // i.e., 3, and size should be 0 as the list is not written.
        // And the last set value is the value currently being written, which is 10.
        assertEquals(5, offSet);
        assertEquals(0, size);
        assertEquals(10, lastSet);
      }

      listViewWriter.bigInt().writeBigInt(12);
      listViewWriter.endList();

      int offSet11 = offSetBuffer.getInt(10 * BaseRepeatedValueViewVector.OFFSET_WIDTH);
      int size11 = sizeBuffer.getInt(10 * BaseRepeatedValueViewVector.SIZE_WIDTH);
      int lastSet11 = listViewVector.getLastSet();

      assertEquals(5, offSet11);
      assertEquals(1, size11);
      assertEquals(10, lastSet11);

      listViewVector.setValueCount(11);
    }
  }

  @Test
  public void testNestedListViewVector() {
    try (ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator)) {
      UnionListViewWriter listViewWriter = listViewVector.getWriter();

      /* allocate memory */
      listViewWriter.allocate();

      /* the dataVector that backs a listVector will also be a
       * listVector for this test.
       */

      /* write one or more inner lists at index 0 */
      listViewWriter.setPosition(0);
      listViewWriter.startList();

      listViewWriter.list().startList();
      listViewWriter.list().bigInt().writeBigInt(50);
      listViewWriter.list().bigInt().writeBigInt(100);
      listViewWriter.list().bigInt().writeBigInt(200);
      listViewWriter.list().endList();

      listViewWriter.list().startList();
      listViewWriter.list().bigInt().writeBigInt(75);
      listViewWriter.list().bigInt().writeBigInt(125);
      listViewWriter.list().bigInt().writeBigInt(150);
      listViewWriter.list().bigInt().writeBigInt(175);
      listViewWriter.list().endList();

      listViewWriter.endList();

      /* write one or more inner lists at index 1 */
      listViewWriter.setPosition(1);
      listViewWriter.startList();

      listViewWriter.list().startList();
      listViewWriter.list().bigInt().writeBigInt(10);
      listViewWriter.list().endList();

      listViewWriter.list().startList();
      listViewWriter.list().bigInt().writeBigInt(15);
      listViewWriter.list().bigInt().writeBigInt(20);
      listViewWriter.list().endList();

      listViewWriter.list().startList();
      listViewWriter.list().bigInt().writeBigInt(25);
      listViewWriter.list().bigInt().writeBigInt(30);
      listViewWriter.list().bigInt().writeBigInt(35);
      listViewWriter.list().endList();

      listViewWriter.endList();

      assertEquals(1, listViewVector.getLastSet());

      listViewVector.setValueCount(2);

      // [[[50,100,200],[75,125,150,175]], [[10],[15,20],[25,30,35]]]

      assertEquals(2, listViewVector.getValueCount());

      /* get listViewVector value at index 0 -- the value itself is a listViewVector */
      Object result = listViewVector.getObject(0);
      ArrayList<ArrayList<Long>> resultSet = (ArrayList<ArrayList<Long>>) result;
      ArrayList<Long> list;

      assertEquals(2, resultSet.size()); /* 2 inner lists at index 0 */
      assertEquals(3, resultSet.get(0).size()); /* size of the first inner list */
      assertEquals(4, resultSet.get(1).size()); /* size of the second inner list */

      list = resultSet.get(0);
      assertEquals(Long.valueOf(50), list.get(0));
      assertEquals(Long.valueOf(100), list.get(1));
      assertEquals(Long.valueOf(200), list.get(2));

      list = resultSet.get(1);
      assertEquals(Long.valueOf(75), list.get(0));
      assertEquals(Long.valueOf(125), list.get(1));
      assertEquals(Long.valueOf(150), list.get(2));
      assertEquals(Long.valueOf(175), list.get(3));

      /* get listViewVector value at index 1 -- the value itself is a listViewVector */
      result = listViewVector.getObject(1);
      resultSet = (ArrayList<ArrayList<Long>>) result;

      assertEquals(3, resultSet.size()); /* 3 inner lists at index 1 */
      assertEquals(1, resultSet.get(0).size()); /* size of the first inner list */
      assertEquals(2, resultSet.get(1).size()); /* size of the second inner list */
      assertEquals(3, resultSet.get(2).size()); /* size of the third inner list */

      list = resultSet.get(0);
      assertEquals(Long.valueOf(10), list.get(0));

      list = resultSet.get(1);
      assertEquals(Long.valueOf(15), list.get(0));
      assertEquals(Long.valueOf(20), list.get(1));

      list = resultSet.get(2);
      assertEquals(Long.valueOf(25), list.get(0));
      assertEquals(Long.valueOf(30), list.get(1));
      assertEquals(Long.valueOf(35), list.get(2));

      /* check underlying bitVector */
      assertFalse(listViewVector.isNull(0));
      assertFalse(listViewVector.isNull(1));

      final ArrowBuf offSetBuffer = listViewVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();

      // check offset buffer
      assertEquals(0, offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(2, offSetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(2, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(3, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));
    }
  }

  @Test
  public void testNestedListVector() throws Exception {
    try (ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator)) {

      MinorType listType = MinorType.LISTVIEW;
      MinorType scalarType = MinorType.BIGINT;

      listViewVector.addOrGetVector(FieldType.nullable(listType.getType()));

      ListViewVector innerList1 = (ListViewVector) listViewVector.getDataVector();
      innerList1.addOrGetVector(FieldType.nullable(listType.getType()));

      ListViewVector innerList2 = (ListViewVector) innerList1.getDataVector();
      innerList2.addOrGetVector(FieldType.nullable(listType.getType()));

      ListViewVector innerList3 = (ListViewVector) innerList2.getDataVector();
      innerList3.addOrGetVector(FieldType.nullable(listType.getType()));

      ListViewVector innerList4 = (ListViewVector) innerList3.getDataVector();
      innerList4.addOrGetVector(FieldType.nullable(listType.getType()));

      ListViewVector innerList5 = (ListViewVector) innerList4.getDataVector();
      innerList5.addOrGetVector(FieldType.nullable(listType.getType()));

      ListViewVector innerList6 = (ListViewVector) innerList5.getDataVector();
      innerList6.addOrGetVector(FieldType.nullable(scalarType.getType()));

      listViewVector.setInitialCapacity(128);
    }
  }

  private void setValuesInBuffer(int[] bufValues, ArrowBuf buffer, long bufWidth) {
    for (int i = 0; i < bufValues.length; i++) {
      buffer.setInt(i * bufWidth, bufValues[i]);
    }
  }

  /**
   * Setting up the buffers directly needs to be validated with the base method used in
   * the ListVector class where we use the approach of startList(),
   * write to the child vector and endList().
   * <p>
   * To support this, we have to consider the following scenarios;
   * <p>
   * 1. Only using directly buffer-based inserts.
   * 2. Default list insertion followed by buffer-based inserts.
   * 3. Buffer-based inserts followed by default list insertion.
   */
  @Test
  public void testBasicListViewSet() {

    ArrowBuf newOffSetBuf = allocator.buffer(1024);
    ArrowBuf newSizeBuffer = allocator.buffer(1024);
    ArrowBuf validityBuffer = allocator.buffer(DataSizeRoundingUtil.divideBy8Ceil(1024));
    BigIntVector elementVector = new BigIntVector("element-vector", allocator);
    ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator);

    listViewVector.allocateNew();

    elementVector.allocateNew(7);

    elementVector.set(0, 12);
    elementVector.set(1, -7);
    elementVector.set(2, 25);
    elementVector.set(3, 0);
    elementVector.set(4, -127);
    elementVector.set(5, 127);
    elementVector.set(6, 50);

    elementVector.setValueCount(7);

    int[] offSetValues = new int[]{0, 3, 3, 7};
    int[] sizeValues = new int[]{3, 0, 4, 0};

    BitVectorHelper.setBit(validityBuffer, 0);
    BitVectorHelper.setBit(validityBuffer, 2);
    BitVectorHelper.setBit(validityBuffer, 3);

    setValuesInBuffer(offSetValues, newOffSetBuf, BaseRepeatedValueViewVector.OFFSET_WIDTH);
    setValuesInBuffer(sizeValues, newSizeBuffer, BaseRepeatedValueViewVector.SIZE_WIDTH);

    listViewVector.set(newOffSetBuf, newSizeBuffer, validityBuffer, elementVector, 4);

    final ArrowBuf offSetBuffer = listViewVector.getOffsetBuffer();
    final ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();

    // check offset buffer
    assertEquals(0, offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
    assertEquals(3, offSetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
    assertEquals(3, offSetBuffer.getInt(2 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
    assertEquals(7, offSetBuffer.getInt(3 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

    // check size buffer
    assertEquals(3, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
    assertEquals(0, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));
    assertEquals(4, sizeBuffer.getInt(2 * BaseRepeatedValueViewVector.SIZE_WIDTH));
    assertEquals(0, sizeBuffer.getInt(3 * BaseRepeatedValueViewVector.SIZE_WIDTH));

    // check values
    assertEquals(12, ((BigIntVector) listViewVector.getDataVector()).get(0));
    assertEquals(-7, ((BigIntVector) listViewVector.getDataVector()).get(1));
    assertEquals(25, ((BigIntVector) listViewVector.getDataVector()).get(2));
    assertEquals(0, ((BigIntVector) listViewVector.getDataVector()).get(3));
    assertEquals(-127, ((BigIntVector) listViewVector.getDataVector()).get(4));
    assertEquals(127, ((BigIntVector) listViewVector.getDataVector()).get(5));
    assertEquals(50, ((BigIntVector) listViewVector.getDataVector()).get(6));

    assertEquals(3, listViewVector.getLastSet());

    listViewVector.close();
  }

  @Test
  public void testBasicListViewSetNested() {
    // Expected listview
    // [[[50,100,200],[75,125,150,175]],[[10],[15,20],[25,30,35]]]

    // Setting child vector
    // [[50,100,200],[75,125,150,175],[10],[15,20],[25,30,35]]
    ListVector listVector = ListVector.empty("nestedVector", allocator);

    UnionListWriter listWriter = listVector.getWriter();
    listWriter.allocate();

    listWriter.setPosition(0);
    listWriter.startList();

    listWriter.bigInt().writeBigInt(50);
    listWriter.bigInt().writeBigInt(100);
    listWriter.bigInt().writeBigInt(200);

    listWriter.endList();

    listWriter.setPosition(1);
    listWriter.startList();

    listWriter.bigInt().writeBigInt(75);
    listWriter.bigInt().writeBigInt(125);
    listWriter.bigInt().writeBigInt(150);
    listWriter.bigInt().writeBigInt(175);

    listWriter.endList();

    listWriter.setPosition(2);
    listWriter.startList();

    listWriter.bigInt().writeBigInt(10);

    listWriter.endList();

    listWriter.startList();
    listWriter.setPosition(3);

    listWriter.bigInt().writeBigInt(15);
    listWriter.bigInt().writeBigInt(20);

    listWriter.endList();

    listWriter.startList();
    listWriter.setPosition(4);

    listWriter.bigInt().writeBigInt(25);
    listWriter.bigInt().writeBigInt(30);
    listWriter.bigInt().writeBigInt(35);

    listWriter.endList();

    listVector.setValueCount(2);

    ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator);
    listViewVector.allocateNew();

    ArrowBuf newOffSetBuf = allocator.buffer(1024);
    ArrowBuf newSizeBuffer = allocator.buffer(1024);
    ArrowBuf validityBuffer = allocator.buffer(DataSizeRoundingUtil.divideBy8Ceil(1024));

    int[] offSetValues = new int[]{0, 2};
    int[] sizeValues = new int[]{2, 3};

    BitVectorHelper.setBit(validityBuffer, 0);
    BitVectorHelper.setBit(validityBuffer, 1);

    setValuesInBuffer(offSetValues, newOffSetBuf, BaseRepeatedValueViewVector.OFFSET_WIDTH);
    setValuesInBuffer(sizeValues, newSizeBuffer, BaseRepeatedValueViewVector.SIZE_WIDTH);

    listViewVector.set(newOffSetBuf, newSizeBuffer, validityBuffer, listVector, 2);

    assertEquals(2, listViewVector.getValueCount());

    /* get listViewVector value at index 0 -- the value itself is a listViewVector */
    Object result = listViewVector.getObject(0);
    ArrayList<ArrayList<Long>> resultSet = (ArrayList<ArrayList<Long>>) result;
    ArrayList<Long> list;

    assertEquals(2, resultSet.size()); /* 2 inner lists at index 0 */
    assertEquals(3, resultSet.get(0).size()); /* size of the first inner list */
    assertEquals(4, resultSet.get(1).size()); /* size of the second inner list */

    list = resultSet.get(0);
    assertEquals(Long.valueOf(50), list.get(0));
    assertEquals(Long.valueOf(100), list.get(1));
    assertEquals(Long.valueOf(200), list.get(2));

    list = resultSet.get(1);
    assertEquals(Long.valueOf(75), list.get(0));
    assertEquals(Long.valueOf(125), list.get(1));
    assertEquals(Long.valueOf(150), list.get(2));
    assertEquals(Long.valueOf(175), list.get(3));

    /* get listViewVector value at index 1 -- the value itself is a listViewVector */
    result = listViewVector.getObject(1);
    resultSet = (ArrayList<ArrayList<Long>>) result;

    assertEquals(3, resultSet.size()); /* 3 inner lists at index 1 */
    assertEquals(1, resultSet.get(0).size()); /* size of the first inner list */
    assertEquals(2, resultSet.get(1).size()); /* size of the second inner list */
    assertEquals(3, resultSet.get(2).size()); /* size of the third inner list */

    list = resultSet.get(0);
    assertEquals(Long.valueOf(10), list.get(0));

    list = resultSet.get(1);
    assertEquals(Long.valueOf(15), list.get(0));
    assertEquals(Long.valueOf(20), list.get(1));

    list = resultSet.get(2);
    assertEquals(Long.valueOf(25), list.get(0));
    assertEquals(Long.valueOf(30), list.get(1));
    assertEquals(Long.valueOf(35), list.get(2));

    /* check underlying bitVector */
    assertFalse(listViewVector.isNull(0));
    assertFalse(listViewVector.isNull(1));

    final ArrowBuf offSetBuffer = listViewVector.getOffsetBuffer();
    final ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();

    // check offset buffer
    assertEquals(0, offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
    assertEquals(2, offSetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

    // check size buffer
    assertEquals(2, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
    assertEquals(3, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));

    listVector.close();
    listViewVector.close();
  }

  @Test
  public void testBasicListViewSetWithListViewWriter() {
    ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator);
    ArrowBuf newOffSetBuf = allocator.buffer(1024);
    ArrowBuf newSizeBuffer = allocator.buffer(1024);
    ArrowBuf validityBuffer = allocator.buffer(DataSizeRoundingUtil.divideBy8Ceil(1024));
    BigIntVector elementVector = new BigIntVector("element-vector", allocator);

    listViewVector.allocateNew();
    elementVector.allocateNew(7);

    elementVector.set(0, 12);
    elementVector.set(1, -7);
    elementVector.set(2, 25);
    elementVector.set(3, 0);
    elementVector.set(4, -127);
    elementVector.set(5, 127);
    elementVector.set(6, 50);

    elementVector.setValueCount(7);

    int[] offSetValues = new int[]{0, 3, 3, 7};
    int[] sizeValues = new int[]{3, 0, 4, 0};

    BitVectorHelper.setBit(validityBuffer, 0);
    BitVectorHelper.setBit(validityBuffer, 2);
    BitVectorHelper.setBit(validityBuffer, 3);

    setValuesInBuffer(offSetValues, newOffSetBuf, BaseRepeatedValueViewVector.OFFSET_WIDTH);
    setValuesInBuffer(sizeValues, newSizeBuffer, BaseRepeatedValueViewVector.SIZE_WIDTH);

    listViewVector.set(newOffSetBuf, newSizeBuffer, validityBuffer, elementVector, 4);

    final ArrowBuf offSetBuffer = listViewVector.getOffsetBuffer();
    final ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();

    // check offset buffer
    assertEquals(0, offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
    assertEquals(3, offSetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
    assertEquals(3, offSetBuffer.getInt(2 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
    assertEquals(7, offSetBuffer.getInt(3 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

    // check size buffer
    assertEquals(3, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
    assertEquals(0, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));
    assertEquals(4, sizeBuffer.getInt(2 * BaseRepeatedValueViewVector.SIZE_WIDTH));
    assertEquals(0, sizeBuffer.getInt(3 * BaseRepeatedValueViewVector.SIZE_WIDTH));

    // check values
    assertEquals(12, ((BigIntVector) listViewVector.getDataVector()).get(0));
    assertEquals(-7, ((BigIntVector) listViewVector.getDataVector()).get(1));
    assertEquals(25, ((BigIntVector) listViewVector.getDataVector()).get(2));
    assertEquals(0, ((BigIntVector) listViewVector.getDataVector()).get(3));
    assertEquals(-127, ((BigIntVector) listViewVector.getDataVector()).get(4));
    assertEquals(127, ((BigIntVector) listViewVector.getDataVector()).get(5));
    assertEquals(50, ((BigIntVector) listViewVector.getDataVector()).get(6));

    assertEquals(3, listViewVector.getLastSet());

    UnionListViewWriter listViewWriter = listViewVector.getWriter();

    listViewWriter.setPosition(4);
    listViewWriter.startList();

    listViewWriter.bigInt().writeBigInt(121);
    listViewWriter.bigInt().writeBigInt(-71);
    listViewWriter.bigInt().writeBigInt(251);
    listViewWriter.endList();

    listViewVector.setValueCount(5);

    // final ArrowBuf offSetBuffer = listViewVector.getOffsetBuffer();
    // final ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();

    // check offset buffer
    assertEquals(0, offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
    assertEquals(3, offSetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
    assertEquals(3, offSetBuffer.getInt(2 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
    assertEquals(7, offSetBuffer.getInt(3 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
    assertEquals(7, offSetBuffer.getInt(4 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

    // check size buffer
    assertEquals(3, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
    assertEquals(0, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));
    assertEquals(4, sizeBuffer.getInt(2 * BaseRepeatedValueViewVector.SIZE_WIDTH));
    assertEquals(0, sizeBuffer.getInt(3 * BaseRepeatedValueViewVector.SIZE_WIDTH));
    assertEquals(3, sizeBuffer.getInt(4 * BaseRepeatedValueViewVector.SIZE_WIDTH));

    // check values
    assertEquals(12, ((BigIntVector) listViewVector.getDataVector()).get(0));
    assertEquals(-7, ((BigIntVector) listViewVector.getDataVector()).get(1));
    assertEquals(25, ((BigIntVector) listViewVector.getDataVector()).get(2));
    assertEquals(0, ((BigIntVector) listViewVector.getDataVector()).get(3));
    assertEquals(-127, ((BigIntVector) listViewVector.getDataVector()).get(4));
    assertEquals(127, ((BigIntVector) listViewVector.getDataVector()).get(5));
    assertEquals(50, ((BigIntVector) listViewVector.getDataVector()).get(6));
    assertEquals(121, ((BigIntVector) listViewVector.getDataVector()).get(7));
    assertEquals(-71, ((BigIntVector) listViewVector.getDataVector()).get(8));
    assertEquals(251, ((BigIntVector) listViewVector.getDataVector()).get(9));

    assertEquals(4, listViewVector.getLastSet());

    listViewVector.close();
  }

  @Test
  public void testGetBufferAddress() throws Exception {
    try (ListViewVector listViewVector = ListViewVector.empty("vector", allocator)) {

      UnionListViewWriter listViewWriter = listViewVector.getWriter();
      boolean error = false;

      listViewWriter.allocate();

      listViewWriter.setPosition(0);
      listViewWriter.startList();
      listViewWriter.bigInt().writeBigInt(50);
      listViewWriter.bigInt().writeBigInt(100);
      listViewWriter.bigInt().writeBigInt(200);
      listViewWriter.endList();

      listViewWriter.setPosition(1);
      listViewWriter.startList();
      listViewWriter.bigInt().writeBigInt(250);
      listViewWriter.bigInt().writeBigInt(300);
      listViewWriter.endList();

      listViewVector.setValueCount(2);

      /* check listVector contents */
      Object result = listViewVector.getObject(0);
      ArrayList<Long> resultSet = (ArrayList<Long>) result;
      assertEquals(3, resultSet.size());
      assertEquals(Long.valueOf(50), resultSet.get(0));
      assertEquals(Long.valueOf(100), resultSet.get(1));
      assertEquals(Long.valueOf(200), resultSet.get(2));

      result = listViewVector.getObject(1);
      resultSet = (ArrayList<Long>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Long.valueOf(250), resultSet.get(0));
      assertEquals(Long.valueOf(300), resultSet.get(1));

      List<ArrowBuf> buffers = listViewVector.getFieldBuffers();

      long bitAddress = listViewVector.getValidityBufferAddress();
      long offsetAddress = listViewVector.getOffsetBufferAddress();
      long sizeAddress = listViewVector.getSizeBufferAddress();

      try {
        listViewVector.getDataBufferAddress();
      } catch (UnsupportedOperationException ue) {
        error = true;
      } finally {
        assertTrue(error);
      }

      assertEquals(3, buffers.size());
      assertEquals(bitAddress, buffers.get(0).memoryAddress());
      assertEquals(offsetAddress, buffers.get(1).memoryAddress());
      assertEquals(sizeAddress, buffers.get(2).memoryAddress());

      /* (3+2)/2 */
      assertEquals(2.5, listViewVector.getDensity(), 0);
    }
  }

  @Test
  public void testConsistentChildName() throws Exception {
    try (ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator)) {
      String emptyListStr = listViewVector.getField().toString();
      assertTrue(emptyListStr.contains(ListVector.DATA_VECTOR_NAME));

      listViewVector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));
      String emptyVectorStr = listViewVector.getField().toString();
      assertTrue(emptyVectorStr.contains(ListVector.DATA_VECTOR_NAME));
    }
  }

  @Test
  public void testSetInitialCapacity() {
    try (final ListViewVector vector = ListViewVector.empty("", allocator)) {
      vector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));

      vector.setInitialCapacity(512);
      vector.allocateNew();
      assertEquals(512, vector.getValueCapacity());
      assertTrue(vector.getDataVector().getValueCapacity() >= 512);

      vector.setInitialCapacity(512, 4);
      vector.allocateNew();
      assertEquals(512, vector.getValueCapacity());
      assertTrue(vector.getDataVector().getValueCapacity() >= 512 * 4);

      vector.setInitialCapacity(512, 0.1);
      vector.allocateNew();
      assertEquals(512, vector.getValueCapacity());
      assertTrue(vector.getDataVector().getValueCapacity() >= 51);

      vector.setInitialCapacity(512, 0.01);
      vector.allocateNew();
      assertEquals(512, vector.getValueCapacity());
      assertTrue(vector.getDataVector().getValueCapacity() >= 5);

      vector.setInitialCapacity(5, 0.1);
      vector.allocateNew();
      assertEquals(8, vector.getValueCapacity());
      assertTrue(vector.getDataVector().getValueCapacity() >= 1);
    }
  }

  @Test
  public void testClearAndReuse() {
    try (final ListViewVector vector = ListViewVector.empty("list", allocator)) {
      BigIntVector bigIntVector =
          (BigIntVector) vector.addOrGetVector(FieldType.nullable(MinorType.BIGINT.getType())).getVector();
      vector.setInitialCapacity(10);
      vector.allocateNew();

      vector.startNewValue(0);
      bigIntVector.setSafe(0, 7);
      vector.endValue(0, 1);
      vector.startNewValue(1);
      bigIntVector.setSafe(1, 8);
      vector.endValue(1, 1);
      vector.setValueCount(2);

      Object result = vector.getObject(0);
      ArrayList<Long> resultSet = (ArrayList<Long>) result;
      assertEquals(Long.valueOf(7), resultSet.get(0));

      result = vector.getObject(1);
      resultSet = (ArrayList<Long>) result;
      assertEquals(Long.valueOf(8), resultSet.get(0));

      // Clear and release the buffers to trigger a realloc when adding next value
      vector.clear();

      // The list vector should reuse a buffer when reallocating the offset buffer
      vector.startNewValue(0);
      bigIntVector.setSafe(0, 7);
      vector.endValue(0, 1);
      vector.startNewValue(1);
      bigIntVector.setSafe(1, 8);
      vector.endValue(1, 1);
      vector.setValueCount(2);

      result = vector.getObject(0);
      resultSet = (ArrayList<Long>) result;
      assertEquals(Long.valueOf(7), resultSet.get(0));

      result = vector.getObject(1);
      resultSet = (ArrayList<Long>) result;
      assertEquals(Long.valueOf(8), resultSet.get(0));
    }
  }

  @Test
  public void testWriterGetField() {
    try (final ListViewVector vector = ListViewVector.empty("listview", allocator)) {

      UnionListViewWriter writer = vector.getWriter();
      writer.allocate();

      //set some values
      writer.startList();
      writer.integer().writeInt(1);
      writer.integer().writeInt(2);
      writer.endList();
      vector.setValueCount(2);

      Field expectedDataField = new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME,
          FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field expectedField = new Field(vector.getName(), FieldType.nullable(ArrowType.ListView.INSTANCE),
          Arrays.asList(expectedDataField));

      assertEquals(expectedField, writer.getField());
    }
  }

  @Test
  public void testWriterUsingHolderGetTimestampMilliTZField() {
    try (final ListViewVector vector = ListViewVector.empty("listview", allocator)) {
      org.apache.arrow.vector.complex.writer.FieldWriter writer = vector.getWriter();
      writer.allocate();

      TimeStampMilliTZHolder holder = new TimeStampMilliTZHolder();
      holder.timezone = "SomeFakeTimeZone";
      writer.startList();
      holder.value = 12341234L;
      writer.timeStampMilliTZ().write(holder);
      holder.value = 55555L;
      writer.timeStampMilliTZ().write(holder);

      // Writing with a different timezone should throw
      holder.timezone = "AsdfTimeZone";
      holder.value = 77777;
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
          () -> writer.timeStampMilliTZ().write(holder));
      assertEquals(
          "holder.timezone: AsdfTimeZone not equal to vector timezone: SomeFakeTimeZone",
          ex.getMessage());

      writer.endList();
      vector.setValueCount(1);

      Field expectedDataField = new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME,
          FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "SomeFakeTimeZone")), null);
      Field expectedField = new Field(vector.getName(), FieldType.nullable(ArrowType.ListView.INSTANCE),
          Arrays.asList(expectedDataField));

      assertEquals(expectedField, writer.getField());
    }
  }

  @Test
  public void testWriterGetDurationField() {
    try (final ListViewVector vector = ListViewVector.empty("listview", allocator)) {
      org.apache.arrow.vector.complex.writer.FieldWriter writer = vector.getWriter();
      writer.allocate();

      DurationHolder durationHolder = new DurationHolder();
      durationHolder.unit = TimeUnit.MILLISECOND;

      writer.startList();
      durationHolder.value = 812374L;
      writer.duration().write(durationHolder);
      durationHolder.value = 143451L;
      writer.duration().write(durationHolder);

      // Writing with a different unit should throw
      durationHolder.unit = TimeUnit.SECOND;
      durationHolder.value = 8888888;
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
          () -> writer.duration().write(durationHolder));
      assertEquals(
          "holder.unit: SECOND not equal to vector unit: MILLISECOND", ex.getMessage());

      writer.endList();
      vector.setValueCount(1);

      Field expectedDataField = new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME,
          FieldType.nullable(new ArrowType.Duration(TimeUnit.MILLISECOND)), null);
      Field expectedField = new Field(vector.getName(),
          FieldType.nullable(ArrowType.ListView.INSTANCE),
          Arrays.asList(expectedDataField));

      assertEquals(expectedField, writer.getField());
    }
  }

  @Test
  public void testClose() throws Exception {
    try (final ListViewVector vector = ListViewVector.empty("listview", allocator)) {

      UnionListViewWriter writer = vector.getWriter();
      writer.allocate();

      //set some values
      writer.startList();
      writer.integer().writeInt(1);
      writer.integer().writeInt(2);
      writer.endList();
      vector.setValueCount(2);

      assertTrue(vector.getBufferSize() > 0);
      assertTrue(vector.getDataVector().getBufferSize() > 0);

      writer.close();
      assertEquals(0, vector.getBufferSize());
      assertEquals(0, vector.getDataVector().getBufferSize());
    }
  }

  @Test
  public void testGetBufferSizeFor() {
    try (final ListViewVector vector = ListViewVector.empty("list", allocator)) {

      UnionListViewWriter writer = vector.getWriter();
      writer.allocate();

      //set some values
      writeIntValues(writer, new int[] {1, 2});
      writeIntValues(writer, new int[] {3, 4});
      writeIntValues(writer, new int[] {5, 6});
      writeIntValues(writer, new int[] {7, 8, 9, 10});
      writeIntValues(writer, new int[] {11, 12, 13, 14});
      writer.setValueCount(5);

      IntVector dataVector = (IntVector) vector.getDataVector();
      int[] indices = new int[] {0, 2, 4, 6, 10, 14};

      for (int valueCount = 1; valueCount <= 5; valueCount++) {
        int validityBufferSize = BitVectorHelper.getValidityBufferSize(valueCount);
        int offsetBufferSize = valueCount * BaseRepeatedValueViewVector.OFFSET_WIDTH;
        int sizeBufferSize = valueCount * BaseRepeatedValueViewVector.SIZE_WIDTH;

        int expectedSize = validityBufferSize + offsetBufferSize + sizeBufferSize +
            dataVector.getBufferSizeFor(indices[valueCount]);
        assertEquals(expectedSize, vector.getBufferSizeFor(valueCount));
      }
    }
  }

  private void writeIntValues(UnionListViewWriter writer, int[] values) {
    writer.startList();
    for (int v: values) {
      writer.integer().writeInt(v);
    }
    writer.endList();
  }

}
