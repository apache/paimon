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
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataFormatTestUtil;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.OutOfRangeException;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.testutils.assertj.AssertionUtils;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ConsumerExpire}. */
public class ConsumerExpireTest {

    @TempDir java.nio.file.Path tempDir;

    private Path path;
    private FileIO fileIO;
    private FileStoreTable table;
    private long commitIdentifier;
    private RowType rowType;

    @BeforeEach
    public void before() {
        this.path = new Path(tempDir.toUri());
        this.fileIO = LocalFileIO.create();
        this.commitIdentifier = 0;
        this.rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
    }

    @Test
    public void test() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.CONSUMER_ID, "my-id");
        options.set(CoreOptions.CONSUMER_EXPIRATION_TIME, Duration.ofHours(1));
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 2);
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 2);

        SchemaManager schemaManager = new SchemaManager(fileIO, path);
        schemaManager.createTable(
                new Schema(
                        rowType.getFields(),
                        Collections.emptyList(),
                        Collections.singletonList("f0"),
                        options.toMap(),
                        ""));
        table = FileStoreTableFactory.create(fileIO, path);

        ReadBuilder readBuilder = table.newReadBuilder();

        StreamTableScan scan1 = readBuilder.newStreamScan();
        write(1, "s1");
        scan1.plan();

        // generate consumer
        Long nextSnapshot = scan1.checkpoint();
        scan1.notifyCheckpointComplete(nextSnapshot);
        ConsumerManager consumerManager = new ConsumerManager(fileIO, path);
        assertThat(consumerManager.consumer("my-id"))
                .map(Consumer::nextSnapshot)
                .get()
                .isEqualTo(2L);

        // read using consumer id
        write(2, "s2");
        StreamTableScan scan2 = readBuilder.newStreamScan();
        assertThat(scan2.plan().splits()).isEmpty();
        List<String> result = getResult(scan2.plan().splits());
        assertThat(result).containsExactlyInAnyOrder("+I[2, s2]");

        // check no expiration
        for (int i = 3; i <= 8; i++) {
            write(i, "s" + i);
        }
        result = getResult(scan2.plan().splits());
        assertThat(result).containsExactlyInAnyOrder("+I[3, s3]");

        // expire consumer
        ConsumerExpire expire = ((AbstractFileStoreTable) table).store().newConsumerExpire();
        assertThat(expire).isNotNull();
        expire.expire(LocalDateTime.now().plusHours(2));

        // commit to trigger snapshot expiration
        write(9, "s9");
        assertThatThrownBy(scan2::plan)
                .satisfies(
                        AssertionUtils.anyCauseMatches(
                                OutOfRangeException.class, "The snapshot with id 4 has expired."));
        SnapshotManager snapshotManager = new SnapshotManager(fileIO, path);
        assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(8);
    }

    private void write(int f0, String f1) throws Exception {
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        try (StreamTableWrite write = writeBuilder.newWrite();
                StreamTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(f0, BinaryString.fromString(f1)));
            commit.commit(commitIdentifier, write.prepareCommit(false, commitIdentifier));
            commitIdentifier++;
        }
    }

    private List<String> getResult(List<Split> splits) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        TableRead read = readBuilder.newRead();

        List<ConcatRecordReader.ReaderSupplier<InternalRow>> readers = new ArrayList<>();
        for (Split split : splits) {
            readers.add(() -> read.createReader(split));
        }
        RecordReader<InternalRow> recordReader = ConcatRecordReader.create(readers);
        RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(recordReader);
        List<String> result = new ArrayList<>();
        while (iterator.hasNext()) {
            InternalRow row = iterator.next();
            result.add(DataFormatTestUtil.internalRowToString(row, rowType));
        }
        iterator.close();
        return result;
    }
}
