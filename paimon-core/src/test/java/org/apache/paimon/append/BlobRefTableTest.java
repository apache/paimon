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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.UriReader;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for table with blob ref. */
public class BlobRefTableTest extends TableTestBase {

    private final byte[] blobBytes = randomBytes();

    @Test
    public void testWriteAndReadBlobRef() throws Exception {
        // 1) write a real BLOB to create a descriptor
        Identifier blobTableId = identifier("BlobTable");
        Schema blobSchema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.BLOB())
                        .option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .build();
        catalog.createTable(blobTableId, blobSchema, true);

        FileStoreTable blobTable = getTable(blobTableId);
        List<BlobDescriptor> descriptors = new ArrayList<>();

        BatchWriteBuilder blobWriteBuilder = blobTable.newBatchWriteBuilder();
        try (BatchTableWrite write = blobWriteBuilder.newWrite();
                BatchTableCommit commit = blobWriteBuilder.newCommit()) {
            write.withBlobConsumer(
                    (blobFieldName, blobDescriptor) -> {
                        descriptors.add(blobDescriptor);
                        return true;
                    });
            write.write(GenericRow.of(1, BinaryString.fromString("a"), new BlobData(blobBytes)));
            commit.commit(write.prepareCommit());
        }

        assertThat(descriptors).hasSize(1);
        BlobDescriptor blobDescriptor = descriptors.get(0);
        assertThat(blobDescriptor).isNotNull();

        // 2) write BlobRef bytes into another table
        Identifier blobRefTableId = identifier("BlobRefTable");
        Schema blobRefSchema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.BLOB_REF())
                        .build();
        catalog.createTable(blobRefTableId, blobRefSchema, true);

        FileStoreTable blobRefTable = getTable(blobRefTableId);
        byte[] blobRefBytes = blobDescriptor.serialize();

        BatchWriteBuilder refWriteBuilder = blobRefTable.newBatchWriteBuilder();
        try (BatchTableWrite write = refWriteBuilder.newWrite();
                BatchTableCommit commit = refWriteBuilder.newCommit()) {
            write.write(GenericRow.of(2, BinaryString.fromString("b"), blobRefBytes));
            commit.commit(write.prepareCommit());
        }

        List<InternalRow> rows = read(blobRefTable);
        assertThat(rows).hasSize(1);
        byte[] readBytes = rows.get(0).getBinary(2);
        assertThat(readBytes).isEqualTo(blobRefBytes);

        // 3) read blob data via descriptor
        UriReader uriReader = UriReader.fromFile(blobTable.fileIO());
        BlobDescriptor readDescriptor = BlobDescriptor.deserialize(readBytes);
        assertThat(Blob.fromDescriptor(uriReader, readDescriptor).toData()).isEqualTo(blobBytes);
    }

    @Test
    public void testInvalidBlobRefBytesRejected() throws Exception {
        Identifier blobRefTableId = identifier("BlobRefTableInvalid");
        Schema blobRefSchema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.BLOB_REF())
                        .build();
        catalog.createTable(blobRefTableId, blobRefSchema, true);

        FileStoreTable blobRefTable = getTable(blobRefTableId);
        byte[] invalid = new byte[] {1, 2, 3};

        assertThatThrownBy(
                        () -> {
                            BatchWriteBuilder refWriteBuilder = blobRefTable.newBatchWriteBuilder();
                            try (BatchTableWrite write = refWriteBuilder.newWrite();
                                    BatchTableCommit commit = refWriteBuilder.newCommit()) {
                                write.write(
                                        GenericRow.of(1, BinaryString.fromString("x"), invalid));
                                commit.commit(write.prepareCommit());
                            }
                        })
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Invalid BlobRef bytes");
    }
}
