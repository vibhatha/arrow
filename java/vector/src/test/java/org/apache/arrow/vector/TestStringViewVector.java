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

import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestStringViewVector {

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  private void showInfo(LargeVarCharVector vector) {
    System.out.println("====================================");
    System.out.println("OffSet Buffer Info");
    System.out.println("Capacity: " + vector.offsetBuffer.capacity());
    System.out.println("WriterIndex: " + vector.offsetBuffer.writerIndex());
    System.out.println("ReaderIndex: " + vector.offsetBuffer.readerIndex());
    System.out.println("ReadableBytes: " + vector.offsetBuffer.readableBytes());

    System.out.println("Value Buffer Info");
    System.out.println("Capacity: " + vector.valueBuffer.capacity());
    System.out.println("WriterIndex: " + vector.valueBuffer.writerIndex());
    System.out.println("ReaderIndex: " + vector.valueBuffer.readerIndex());
    System.out.println("ReadableBytes: " + vector.valueBuffer.readableBytes());
    System.out.println("====================================");
  }

  @Test
  public void basicTest() {
    System.out.println(">>>> Running basicTest <<<<");
    final byte[] STR1 = "12345678".getBytes();
    final byte[] STR2 = "1234".getBytes();
    final byte[] STR3 = "12".getBytes();
    try (BufferAllocator childAllocator1 = allocator.newChildAllocator("child1", 1000000, 1000000);
        final LargeVarCharVector vector = new LargeVarCharVector("myvector1", childAllocator1)) {
      vector.allocateNew(3);

      System.out.println("Initial Information");
      showInfo(vector);

      vector.set(0, STR1);
      System.out.println("After First Allocation");
      showInfo(vector);
      byte [] bytes = vector.get(0);
      System.out.println("Read Bytes");
      for (byte b : bytes) {
        System.out.print(b + "\n");
      }
      assertEquals(STR1.length, bytes.length);

      vector.setValueCount(1);
      System.out.println("After First Allocation and Set Value Count");
      showInfo(vector);

      vector.set(1, STR2);
      vector.setValueCount(2);

      System.out.println("After Second Allocation and Set Value count");
      showInfo(vector);

      vector.set(2, STR3);
      vector.setValueCount(3);

      System.out.println("After Third Allocation and Set Value Count");
      showInfo(vector);
    }
  }
}
