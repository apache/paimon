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

package org.apache.paimon.table.format;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.UriReader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for BLOB {@link FormatTable}. */
public class FormatTableBlobTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    public void testReadAndWriteBlobDataAndDescriptor() throws Exception {
        FormatTable table = createBlobFormatTable();
        byte[] first = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] second = "world".getBytes(StandardCharsets.UTF_8);
        Blob descriptorBlob = createDescriptorBlob(table, second);

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(new BlobData(first), 1));
            write.write(GenericRow.of(descriptorBlob, 1));
            write.write(GenericRow.of(null, 1));
            commit.commit(write.prepareCommit());
        }

        assertReadRows(table, first, second);
    }

    @Test
    public void testBlobConsumerProducesDescriptors() throws Exception {
        FormatTable table = createBlobFormatTable();
        byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
        List<BlobDescriptor> descriptors = new ArrayList<>();

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.withBlobConsumer(
                    (blobFieldName, blobDescriptor) -> {
                        assertThat(blobFieldName).isEqualTo("payload");
                        descriptors.add(blobDescriptor);
                        return true;
                    });
            write.write(GenericRow.of(new BlobData(bytes), 1));
            write.write(GenericRow.of(null, 1));
            commit.commit(write.prepareCommit());
        }

        assertThat(descriptors).hasSize(2);
        assertDescriptor(table, descriptors.get(0), bytes);
        assertThat(descriptors.get(1)).isNull();
    }

    private FormatTable createBlobFormatTable() throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("payload", DataTypes.BLOB())
                        .field("ds", DataTypes.INT())
                        .build();
        java.nio.file.Path tableDir = tempPath.resolve("table");
        Files.createDirectories(tableDir);
        Path tablePath = new Path(tableDir.toUri());
        Map<String, String> options = new HashMap<>();
        options.put("path", tablePath.toString());
        options.put("file.format", "blob");

        return FormatTable.builder()
                .fileIO(LocalFileIO.create())
                .identifier(Identifier.create("test_db", "blob_table"))
                .rowType(rowType)
                .partitionKeys(Collections.singletonList("ds"))
                .location(tablePath.toString())
                .format(FormatTable.Format.BLOB)
                .options(options)
                .build();
    }

    private Blob createDescriptorBlob(FormatTable table, byte[] bytes) throws Exception {
        java.nio.file.Path externalFile = tempPath.resolve("external-blob");
        Files.write(externalFile, bytes);
        BlobDescriptor descriptor =
                new BlobDescriptor(new Path(externalFile.toUri()).toString(), 0, bytes.length);
        return Blob.fromDescriptor(UriReader.fromFile(table.fileIO()), descriptor);
    }

    private void assertDescriptor(FormatTable table, BlobDescriptor descriptor, byte[] bytes)
            throws Exception {
        assertThat(descriptor).isNotNull();
        assertThat(descriptor.uri()).isNotEmpty();
        assertThat(descriptor.uri()).contains("ds=1");
        assertThat(descriptor.offset()).isGreaterThanOrEqualTo(0L);
        assertThat(descriptor.length()).isEqualTo(bytes.length);
        assertThat(Blob.fromDescriptor(UriReader.fromFile(table.fileIO()), descriptor).toData())
                .isEqualTo(bytes);
    }

    private void assertReadRows(FormatTable table, byte[] first, byte[] second) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            InternalRowSerializer serializer = new InternalRowSerializer(readBuilder.readType());
            reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
        }

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getBlob(0).toData()).isEqualTo(first);
        assertThat(rows.get(0).getInt(1)).isEqualTo(1);
        assertThat(rows.get(1).getBlob(0).toData()).isEqualTo(second);
        assertThat(rows.get(1).getInt(1)).isEqualTo(1);
        assertThat(rows.get(2).isNullAt(0)).isTrue();
        assertThat(rows.get(2).getInt(1)).isEqualTo(1);
    }
}
