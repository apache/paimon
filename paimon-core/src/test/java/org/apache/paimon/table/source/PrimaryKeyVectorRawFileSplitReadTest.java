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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests raw-file materialization of primary-key vector results. */
class PrimaryKeyVectorRawFileSplitReadTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testRegularLimitDoesNotDropSelectedPhysicalPosition() throws Exception {
        Path tablePath = new Path(tempDir.toUri());
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        options.set(CoreOptions.BUCKET, 1);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.INT())
                        .primaryKey("id")
                        .options(options.toMap())
                        .build();
        TableSchema tableSchema =
                SchemaUtils.forceCommit(new SchemaManager(LocalFileIO.create(), tablePath), schema);
        FileStoreTable table =
                FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < 5; i++) {
                write.write(GenericRow.of(i, i * 10));
            }
            commit.commit(write.prepareCommit());
        }

        DataSplit dataSplit = table.newSnapshotReader().read().dataSplits().get(0);
        assertThat(dataSplit.dataFiles()).hasSize(1);
        Map<Integer, Float> scores = new HashMap<>();
        scores.put(3, 0.25F);
        PrimaryKeyVectorDataSplit vectorSplit =
                new PrimaryKeyVectorDataSplit(dataSplit, RoaringBitmap32.bitmapOf(3), scores);

        try (RecordReader<InternalRow> reader =
                table.newRead().withLimit(1).createReader(vectorSplit)) {
            ScoreRecordIterator<InternalRow> batch =
                    (ScoreRecordIterator<InternalRow>) reader.readBatch();
            InternalRow row = batch.next();
            assertThat(row.getInt(0)).isEqualTo(3);
            assertThat(row.getInt(1)).isEqualTo(30);
            assertThat(batch.returnedRowId()).isEqualTo(3);
            assertThat(batch.returnedScore()).isEqualTo(0.25F);
            assertThat(batch.next()).isNull();
            batch.releaseBatch();
            assertThat(reader.readBatch()).isNull();
        }
    }
}
