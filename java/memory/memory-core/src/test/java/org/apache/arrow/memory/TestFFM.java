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
package org.apache.arrow.memory;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import org.apache.arrow.memory.util.MemoryUtil;
import org.junit.jupiter.api.Test;

public class TestFFM {

  @Test
  public void testBufferCopyWithUnsafe() {
    ByteBuffer srcBuffer = ByteBuffer.allocateDirect(10);
    ByteBuffer destBuffer = ByteBuffer.allocateDirect(10);

    // Fill source buffer with data
    for (int i = 0; i < 10; i++) {
      srcBuffer.put((byte) i);
    }

    // Flip the buffer to prepare for reading
    srcBuffer.flip();

    // Get the memory addresses of the buffers
    long srcAddress = MemoryUtil.getByteBufferAddress(srcBuffer);
    long destAddress = MemoryUtil.getByteBufferAddress(destBuffer);

    // Copy the data from the source buffer to the destination buffer
    MemoryUtil.copyMemory(srcAddress, destAddress, 10);
    // Verify the copy
    destBuffer.flip();
    while (destBuffer.hasRemaining()) {
      System.out.println(destBuffer.get());
    }
  }

  @Test
  public void testBufferCopyWithFFM() {
    // Allocate two ByteBuffers
    ByteBuffer srcBuffer = ByteBuffer.allocateDirect(10);
    ByteBuffer destBuffer = ByteBuffer.allocateDirect(10);

    // Fill source buffer with data
    for (int i = 0; i < 10; i++) {
      srcBuffer.put((byte) i);
    }

    // Flip the source buffer to prepare for reading
    srcBuffer.flip();

    // Wrap the ByteBuffers with MemorySegments
    MemorySegment srcSegment = MemorySegment.ofBuffer(srcBuffer);
    MemorySegment destSegment = MemorySegment.ofBuffer(destBuffer);

    // Copy memory from srcSegment to destSegment
    srcSegment.copyFrom(destSegment.asSlice(0, 10));

    // Verify the copy
    destBuffer.flip();
    while (destBuffer.hasRemaining()) {
      System.out.println(destBuffer.get());
    }
  }
}
