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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test that a full compaction which rewrites data files also cleans up the consumed deletion vector
 * index entries, instead of leaving orphan entries pointing to dead data files.
 *
 * <p>The scenario mirrors the write-only + standalone compaction pipeline:
 *
 * <pre>
 *   1. write rows, full compact         -> data at max level
 *   2. write -D rows (delete by pk)     -> -D records in L0 (write-only, no inline compact)
 *   3. minor compact                    -> DV generated for max-level files
 *   4. full compact                     -> data files rewritten, DV should be gone
 * </pre>
 */
public class DeletionVectorIndexCleanupTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                    new String[] {"pt", "a", "b"});

    @TempDir java.nio.file.Path tempDir;

    private Path tablePath;
    private String commitUser;

    @BeforeEach
    public void before() {
        tablePath = new Path(TraceableFileIO.SCHEME + "://" + tempDir);
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testFullCompactionCleansDvIndex() throws Exception {
        FileStoreTable table =
                createTable(
                        options -> {
                            options.set(BUCKET, 1);
                            options.set(DELETION_VECTORS_ENABLED, true);
                            // mirror production: writer does no inline compaction
                            options.set(CoreOptions.WRITE_ONLY, true);
                        });
        // standalone compaction task view of the same table (write-only forced off,
        // same as what CompactProcedure/CompactAction do)
        FileStoreTable compactTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "false"));

        BinaryRow part = partition(1);

        // step 1: write base data, then full compact to push it to max level
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            for (int i = 0; i < 100; i++) {
                write.write(GenericRow.of(1, i, (long) i));
            }
            commit.commit(0, write.prepareCommit(true, 0));
        }
        compact(compactTable, part, true, 1);

        // step 2: delete some keys, -D records stay in L0 (write-only)
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            for (int i = 0; i < 10; i++) {
                write.write(GenericRow.ofKind(RowKind.DELETE, 1, i, (long) i));
            }
            commit.commit(2, write.prepareCommit(true, 2));
        }

        // step 3: minor compact, DV is generated for the untouched max-level file
        compact(compactTable, part, false, 3);
        assertThat(scanDvEntries(table))
                .as("minor compaction should generate DV for untouched max-level files")
                .isNotEmpty();

        // step 4: full compact, all files carrying DV are rewritten, DV must be cleaned
        compact(compactTable, part, true, 4);

        Set<String> aliveFiles =
                table.newSnapshotReader().read().dataSplits().stream()
                        .flatMap(s -> s.dataFiles().stream())
                        .map(DataFileMeta::fileName)
                        .collect(Collectors.toSet());

        List<String> orphanDvFiles =
                scanDvEntries(table).stream()
                        .filter(
                                entry ->
                                        entry.indexFile().dvRanges() != null
                                                && !entry.indexFile().dvRanges().isEmpty()
                                                && entry.indexFile().dvRanges().keySet().stream()
                                                        .noneMatch(aliveFiles::contains))
                        .map(entry -> entry.indexFile().fileName())
                        .collect(Collectors.toList());

        assertThat(orphanDvFiles)
                .as(
                        "full compaction rewrote all data files, no DV index entry should "
                                + "point to dead files")
                .isEmpty();
    }

    private void compact(
            FileStoreTable compactTable,
            BinaryRow partition,
            boolean fullCompaction,
            long identifier)
            throws Exception {
        try (StreamTableWrite write =
                        compactTable
                                .newWrite(commitUser)
                                .withIOManager(new IOManagerImpl(tempDir.toString()));
                StreamTableCommit commit = compactTable.newCommit(commitUser)) {
            write.compact(partition, 0, fullCompaction);
            commit.commit(identifier, write.prepareCommit(true, identifier));
        }
    }

    private List<IndexManifestEntry> scanDvEntries(FileStoreTable table) {
        return table.store().newIndexFileHandler().scan(DELETION_VECTORS_INDEX);
    }

    private FileStoreTable createTable(Consumer<Options> configure) throws Exception {
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        configure.accept(options);
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "a"),
                                options.toMap(),
                                ""));
        return FileStoreTableFactory.create(FileIOFinder.find(tablePath), tablePath, schema);
    }

    private BinaryRow partition(int pt) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, pt);
        writer.complete();
        return row;
    }
}
