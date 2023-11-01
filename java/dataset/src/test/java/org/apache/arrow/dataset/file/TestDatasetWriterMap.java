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

package org.apache.arrow.dataset.file;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.dataset.TestDataset;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import org.junit.rules.TemporaryFolder;

public class TestDatasetWriterMap extends TestDataset {

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  private VectorSchemaRoot generateAllTypesVector(BufferAllocator allocator) {
    // Notes:
    // - Float16 is not supported by Java.
    // - IntervalMonthDayNano is not supported by Parquet.
    // - Map (GH-38250) and SparseUnion are resulting in serialization errors when writing with the Dataset API.
    // "Unhandled type for Arrow to Parquet schema conversion" errors: IntervalDay, IntervalYear, DenseUnion
    List<Field> fields = List.of(
        new Field("map", FieldType.nullable(new ArrowType.Map(/*keyssorted*/ false)),
            Collections.singletonList(new Field("items", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                Arrays.asList(Field.notNullable("keys", new ArrowType.Int(32, true)),
                    Field.nullable("values", new ArrowType.Int(32, true))))))
    );
    VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator);
    root.allocateNew();
    root.setRowCount(2);

    MapVector mapVector = ((MapVector) root.getVector("map"));

    UnionMapWriter mapWriter = mapVector.getWriter();
    for (int i = 0; i < 2; i++) {
      mapWriter.startMap();
      for (int j = 0; j < i + 1; j++) {
        mapWriter.startEntry();
        mapWriter.key().integer().writeInt(j);
        mapWriter.value().integer().writeInt(j);
        mapWriter.endEntry();
      }
      mapWriter.endMap();
    }
    return root;
  }

  private byte[] serializeFile(VectorSchemaRoot root) {
    try (
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        WritableByteChannel channel = Channels.newChannel(out);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, channel)
    ) {
      writer.start();
      writer.writeBatch();
      writer.end();
      return out.toByteArray();
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to serialize arrow file", e);
    }
  }

  @Test
  @Disabled("Test Disabled")
  public void testAllTypesParquet() throws Exception {
    try (VectorSchemaRoot root = generateAllTypesVector(rootAllocator())) {
      byte[] featherData = serializeFile(root);
      try (SeekableByteChannel channel = new ByteArrayReadableSeekableByteChannel(featherData)) {
        try (ArrowStreamReader reader = new ArrowStreamReader(channel, rootAllocator())) {
          TMP.create();
          final File writtenFolder = TMP.newFolder();
          final String writtenParquet = writtenFolder.toURI().toString();
          DatasetFileWriter.write(rootAllocator(), reader, FileFormat.PARQUET, writtenParquet);
        }
      }
    }
  }
}
