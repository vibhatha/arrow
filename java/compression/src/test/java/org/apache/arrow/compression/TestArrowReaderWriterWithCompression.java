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

package org.apache.arrow.compression;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.GenerateSampleData;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestArrowReaderWriterWithCompression {

  private BufferAllocator allocator;
  private ByteArrayOutputStream out;
  private VectorSchemaRoot root;
  private VarCharVector dictionaryVector;
  private DictionaryProvider.MapDictionaryProvider provider;

  @BeforeEach
  public void setup() {
    if (allocator == null) {
      allocator = new RootAllocator(Integer.MAX_VALUE);
    }
    out = new ByteArrayOutputStream();
    root = null;
    dictionaryVector = (VarCharVector)
            FieldType.nullable(new ArrowType.Utf8()).createNewSingleVector("f1_file", allocator, null);
    Dictionary dictionary = createDictionary(dictionaryVector);
    provider = new DictionaryProvider.MapDictionaryProvider();
    provider.put(dictionary);
  }

  @AfterEach
  public void tearDown() {
    closeQuietly(root, out, dictionaryVector, provider, allocator);
  }

  private void closeQuietly(AutoCloseable... closeables) {
    for (AutoCloseable closeable : closeables) {
      if (closeable != null) {
        try {
          closeable.close();
        } catch (Exception e) {
          // Log the exception
          e.printStackTrace();
        }
      }
    }
  }

  private void setVectorSchemaRoot() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("col", FieldType.notNullable(new ArrowType.Utf8()), new ArrayList<>()));
    root = VectorSchemaRoot.create(new Schema(fields), allocator);

    final int rowCount = 10;
    GenerateSampleData.generateTestData(root.getVector(0), rowCount);
    root.setRowCount(rowCount);
  }

  private void createAndWrite(DictionaryProvider provider, CompressionUtil.CodecType codecType, boolean isFileStream)
          throws IOException {
    setVectorSchemaRoot();
    final Integer compressionLevel = 7;
    try (final ArrowWriter writer = isFileStream ? new ArrowStreamWriter(root, provider, Channels.newChannel(out),
            IpcOption.DEFAULT,
            CommonsCompressionFactory.INSTANCE, codecType, Optional.of(compressionLevel))
            : new ArrowFileWriter(root, provider, Channels.newChannel(out), new HashMap<>(), IpcOption.DEFAULT,
            CommonsCompressionFactory.INSTANCE, codecType, Optional.of(compressionLevel))) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }
  }

  private void createAndWriteArrowFile(DictionaryProvider provider, CompressionUtil.CodecType codecType)
          throws IOException {
    createAndWrite(provider, codecType, false);
  }

  private void createAndWriteArrowStream(DictionaryProvider provider, CompressionUtil.CodecType codecType)
          throws IOException {
    createAndWrite(provider, codecType, true);
  }

  private Dictionary createDictionary(VarCharVector dictionaryVector) {
    setVector(dictionaryVector,
        "foo".getBytes(StandardCharsets.UTF_8),
        "bar".getBytes(StandardCharsets.UTF_8),
        "baz".getBytes(StandardCharsets.UTF_8));

    return new Dictionary(dictionaryVector,
        new DictionaryEncoding(/*id=*/1L, /*ordered=*/false, /*indexType=*/null));
  }

  @Test
  public void testArrowFileZstdRoundTrip() throws Exception {
    // testing with no DictionaryProvider
    createAndWriteArrowFile(/*provider*/null, CompressionUtil.CodecType.ZSTD);
    // with compression
    try (ArrowFileReader reader =
        new ArrowFileReader(new ByteArrayReadableSeekableByteChannel(out.toByteArray()), allocator,
            CommonsCompressionFactory.INSTANCE)) {
      Assertions.assertEquals(1, reader.getRecordBlocks().size());
      Assertions.assertTrue(reader.loadNextBatch());
      Assertions.assertTrue(root.equals(reader.getVectorSchemaRoot()));
      Assertions.assertFalse(reader.loadNextBatch());
    }
    // without compression
    try (ArrowFileReader reader =
        new ArrowFileReader(new ByteArrayReadableSeekableByteChannel(out.toByteArray()), allocator,
            NoCompressionCodec.Factory.INSTANCE)) {
      Assertions.assertEquals(1, reader.getRecordBlocks().size());
      Exception exception = Assert.assertThrows(IllegalArgumentException.class,
          reader::loadNextBatch);
      Assertions.assertEquals("Please add arrow-compression module to use CommonsCompressionFactory for ZSTD",
              exception.getMessage());
    }
  }

  @Test
  public void testArrowStreamZstdRoundTrip() throws Exception {
    // testing with no DictionaryProvider
    createAndWriteArrowStream(/*provider*/null, CompressionUtil.CodecType.ZSTD);
    // with compression
    try (ArrowStreamReader reader =
                 new ArrowStreamReader(new ByteArrayReadableSeekableByteChannel(out.toByteArray()), allocator,
                         CommonsCompressionFactory.INSTANCE)) {
      Assertions.assertTrue(reader.loadNextBatch());
      Assertions.assertTrue(root.equals(reader.getVectorSchemaRoot()));
      Assertions.assertFalse(reader.loadNextBatch());
    }
    // without compression
    try (ArrowStreamReader reader =
                 new ArrowStreamReader(new ByteArrayReadableSeekableByteChannel(out.toByteArray()), allocator,
                         NoCompressionCodec.Factory.INSTANCE)) {
      Exception exception = Assert.assertThrows(IllegalArgumentException.class,
              reader::loadNextBatch);
      Assertions.assertEquals("Please add arrow-compression module to use CommonsCompressionFactory for ZSTD",
              exception.getMessage());
    }
  }

  @Test
  public void testArrowFileZstdRoundTripWithDictionary() throws Exception {
    createAndWriteArrowFile(provider, CompressionUtil.CodecType.ZSTD);

    // with compression
    try (ArrowFileReader reader =
        new ArrowFileReader(new ByteArrayReadableSeekableByteChannel(out.toByteArray()), allocator,
            CommonsCompressionFactory.INSTANCE)) {
      Assertions.assertEquals(1, reader.getRecordBlocks().size());
      Assertions.assertTrue(reader.loadNextBatch());
      Assertions.assertTrue(root.equals(reader.getVectorSchemaRoot()));
      Assertions.assertFalse(reader.loadNextBatch());
    }
    // without compression
    try (ArrowFileReader reader =
        new ArrowFileReader(new ByteArrayReadableSeekableByteChannel(out.toByteArray()), allocator,
            NoCompressionCodec.Factory.INSTANCE)) {
      Assertions.assertEquals(1, reader.getRecordBlocks().size());
      Exception exception = Assert.assertThrows(IllegalArgumentException.class,
          reader::loadNextBatch);
      Assertions.assertEquals("Please add arrow-compression module to use CommonsCompressionFactory for ZSTD",
              exception.getMessage());
    }
  }

  @Test
  public void testArrowStreamZstdRoundTripWithDictionary() throws Exception {
    createAndWriteArrowStream(provider, CompressionUtil.CodecType.ZSTD);

    // with compression
    try (ArrowStreamReader reader =
                 new ArrowStreamReader(new ByteArrayReadableSeekableByteChannel(out.toByteArray()), allocator,
                         CommonsCompressionFactory.INSTANCE)) {
      Assertions.assertTrue(reader.loadNextBatch());
      Assertions.assertTrue(root.equals(reader.getVectorSchemaRoot()));
      Assertions.assertFalse(reader.loadNextBatch());
    }
    // without compression
    try (ArrowStreamReader reader =
                 new ArrowStreamReader(new ByteArrayReadableSeekableByteChannel(out.toByteArray()), allocator,
                         NoCompressionCodec.Factory.INSTANCE)) {
      Exception exception = Assert.assertThrows(IllegalArgumentException.class,
              reader::loadNextBatch);
      Assertions.assertEquals("Please add arrow-compression module to use CommonsCompressionFactory for ZSTD",
              exception.getMessage());
    }
  }

  private static void setVector(VarCharVector vector, byte[]... values) {
    final int length = values.length;
    vector.allocateNewSafe();
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

}
