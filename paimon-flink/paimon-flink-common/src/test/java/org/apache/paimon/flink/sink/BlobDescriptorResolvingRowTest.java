/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.utils.InternalRowTypeSerializer;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.UriReaderFactory;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BlobDescriptorResolvingRow}. */
class BlobDescriptorResolvingRowTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    void testArrayBlobAfterFlinkSerialization() throws Exception {
        byte[] expected = new byte[] {1, 2, 3};
        java.nio.file.Path blobPath = tempPath.resolve("blob");
        Files.write(blobPath, expected);

        RowType rowType = RowType.of(DataTypes.ARRAY(DataTypes.BLOB()));
        GenericRow row =
                GenericRow.of(
                        new GenericArray(
                                new Object[] {
                                    Blob.fromFile(LocalFileIO.create(), blobPath.toUri().toString())
                                }));
        InternalRow serialized = serializeAndDeserialize(row, rowType);

        BlobDescriptorResolvingRow resolvingRow =
                new BlobDescriptorResolvingRow(
                        serialized, UriReaderFactory.fromFileIO(LocalFileIO.create()));

        assertThat(resolvingRow.getArray(0).getBlob(0).toData()).isEqualTo(expected);
    }

    private static InternalRow serializeAndDeserialize(InternalRow row, RowType rowType)
            throws Exception {
        InternalRowTypeSerializer serializer = new InternalRowTypeSerializer(rowType);
        DataOutputSerializer output = new DataOutputSerializer(100);
        serializer.serialize(row, output);
        return serializer.deserialize(new DataInputDeserializer(output.wrapAsByteBuffer()));
    }
}
