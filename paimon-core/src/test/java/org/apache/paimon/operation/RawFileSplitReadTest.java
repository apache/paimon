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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RawFileSplitRead}. */
class RawFileSplitReadTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void readerMappingIsNotSharedBetweenReadTypes() throws Exception {
        Path tablePath = new Path(tempDir.resolve("mapping-cache").toUri());
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.BUCKET_KEY, "first");
        Schema schema =
                Schema.newBuilder()
                        .column("first", DataTypes.STRING())
                        .column("second", DataTypes.INT())
                        .options(options.toMap())
                        .build();
        TableSchema tableSchema =
                SchemaUtils.forceCommit(new SchemaManager(LocalFileIO.create(), tablePath), schema);
        FileStoreTable table =
                FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(BinaryString.fromString("value"), 42));
            commit.commit(write.prepareCommit());
        }

        DataSplit split = table.newSnapshotReader().read().dataSplits().get(0);
        InnerTableRead read = table.newRead();

        RowType firstProjection = table.rowType().project("first");
        read.withReadType(firstProjection);
        try (RecordReader<InternalRow> reader = read.createReader(split)) {
            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.next().getString(0).toString()).isEqualTo("value");
            assertThat(batch.next()).isNull();
            batch.releaseBatch();
        }

        RowType secondProjection = table.rowType().project("second");
        read.withReadType(secondProjection);
        try (RecordReader<InternalRow> reader = read.createReader(split)) {
            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isNotNull();
            InternalRow row = batch.next();
            assertThat(row).isNotNull();
            assertThat(row.getFieldCount()).isEqualTo(1);
            assertThat(row.getInt(0)).isEqualTo(42);
            assertThat(batch.next()).isNull();
            batch.releaseBatch();
        }
    }
}
