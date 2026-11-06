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

package org.apache.paimon.iceberg;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * When Iceberg metadata is created from scratch for a primary key table, buckets with level-0 files
 * or overlapping key ranges produce {@link org.apache.paimon.table.source.DataSplit}s that are not
 * raw-convertible. Such splits must not be dropped wholesale: the files in them that the
 * incremental commit path would have published (via {@code shouldAddFileToIceberg}) must still be
 * exported, otherwise their rows silently vanish from Iceberg until some future compaction happens
 * to rewrite the files.
 */
public class IcebergBootstrapNonRawSplitsTest {

    @TempDir java.nio.file.Path tempDir;

    private FileStoreTable table;
    private TableWriteImpl<?> write;
    private TableCommitImpl commit;
    private String commitUser;

    @Test
    public void testCreateFromScratchExportsCompactedFilesFromNonRawSplits() throws Exception {
        createPrimaryKeyTableWithoutIceberg();
        // snapshot 1: level-0 file {1, 2, 3}
        writeCommit(1, false, GenericRow.of(1, 10), GenericRow.of(2, 20), GenericRow.of(3, 30));
        // snapshot 2: full compaction, everything at max level
        fullCompact(2);
        // snapshot 3: level-0 file {1, 4} overlapping the max level file
        writeCommit(3, false, GenericRow.of(1, 100), GenericRow.of(4, 40));

        enableIceberg(false);
        // snapshot 4: another level-0 file; triggers creating Iceberg metadata from scratch
        writeCommit(4, false, GenericRow.of(5, 50));

        // The bucket's files (max level + two level-0) form a split that is not raw-convertible.
        // The max level file must still be exported: Iceberg sees the data as of the last full
        // compaction, exactly like an incremental sync running since the table was created.
        IcebergMetadata metadata = readMetadata(4);
        assertThat(metadata.snapshots()).hasSize(1);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)", "Record(3, 30)");

        // a full compaction exports the remaining rows through the incremental path
        fullCompact(5);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 100)",
                        "Record(2, 20)",
                        "Record(3, 30)",
                        "Record(4, 40)",
                        "Record(5, 50)");
    }

    @Test
    public void testFullHistoryReplayWithNonRawSplits() throws Exception {
        createPrimaryKeyTableWithoutIceberg();
        writeCommit(1, false, GenericRow.of(1, 10), GenericRow.of(2, 20), GenericRow.of(3, 30));
        fullCompact(2);
        writeCommit(3, false, GenericRow.of(1, 100), GenericRow.of(4, 40));

        enableIceberg(true);
        writeCommit(4, false, GenericRow.of(5, 50));

        // the replay mirrors live commits: every retained snapshot becomes an Iceberg snapshot
        IcebergMetadata metadata = readMetadata(4);
        assertThat(
                        metadata.snapshots().stream()
                                .map(IcebergSnapshot::snapshotId)
                                .collect(Collectors.toList()))
                .containsExactly(1L, 2L, 3L, 4L);

        // snapshot 1 is a single level-0 file with no other files to merge with, so it is
        // raw-convertible and fully visible; snapshots 3 and 4 add level-0 files, which stay
        // invisible until compaction, exactly like live incremental commits
        assertThat(
                        getIcebergResult(
                                icebergTable ->
                                        IcebergGenerics.read(icebergTable).useSnapshot(1).build(),
                                Record::toString))
                .containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)", "Record(3, 30)");
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)", "Record(3, 30)");

        fullCompact(5);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 100)",
                        "Record(2, 20)",
                        "Record(3, 30)",
                        "Record(4, 40)",
                        "Record(5, 50)");
    }

    // ------------------------------------------------------------------------
    //  Utils
    // ------------------------------------------------------------------------

    private void createPrimaryKeyTableWithoutIceberg() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});

        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.FILE_FORMAT, "avro");
        Schema schema =
                new Schema(
                        rowType.getFields(),
                        Collections.emptyList(),
                        Arrays.asList("k"),
                        options.toMap(),
                        "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            table = (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }

        commitUser = UUID.randomUUID().toString();
        write = table.newWrite(commitUser);
        commit = table.newCommit(commitUser);
    }

    private void enableIceberg(boolean syncFullHistory) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(
                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                IcebergOptions.StorageType.TABLE_LOCATION.toString());
        options.put(IcebergOptions.SYNC_FULL_HISTORY.key(), String.valueOf(syncFullHistory));
        options.put(IcebergOptions.METADATA_DELETE_AFTER_COMMIT.key(), "false");
        table = table.copy(options);
        write.close();
        write = table.newWrite(commitUser);
        commit.close();
        commit = table.newCommit(commitUser);
    }

    private void writeCommit(long identifier, boolean waitCompaction, GenericRow... rows)
            throws Exception {
        for (GenericRow row : rows) {
            write.write(row);
        }
        commit.commit(identifier, write.prepareCommit(waitCompaction, identifier));
    }

    private void fullCompact(long identifier) throws Exception {
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        writeCommit(identifier, true);
    }

    private IcebergMetadata readMetadata(long snapshotId) {
        return IcebergMetadata.fromPath(
                table.fileIO(),
                new Path(table.location(), "metadata/v" + snapshotId + ".metadata.json"));
    }

    private List<String> getIcebergResult() throws Exception {
        return getIcebergResult(
                icebergTable -> IcebergGenerics.read(icebergTable).build(), Record::toString);
    }

    private List<String> getIcebergResult(
            Function<org.apache.iceberg.Table, CloseableIterable<Record>> query,
            Function<Record, String> icebergRecordToString)
            throws Exception {
        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of("mydb.db", "t");
        org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(icebergIdentifier);
        CloseableIterable<Record> result = query.apply(icebergTable);
        List<String> actual = new ArrayList<>();
        for (Record record : result) {
            actual.add(icebergRecordToString.apply(record));
        }
        result.close();
        return actual;
    }
}
