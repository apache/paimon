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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CloseableIterator;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Direct DV DELETE uses DataIncrement for both append and supported primary-key table paths. */
class BucketedDeletionVectorDmlTest {

    @TempDir java.nio.file.Path tempDir;

    /**
     * Two deletes restore the same bucket DV and mark different physical rows. The second stale
     * OVERWRITE must fail; recomputing it from the new snapshot must retain both deletions.
     */
    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testConcurrentDeletesAndRecomputation(boolean primaryKey, boolean existingDv)
            throws Exception {
        try (Catalog catalog =
                        CatalogFactory.createCatalog(
                                CatalogContext.create(new Path(tempDir.toString())));
                IOManager ioManager = IOManager.create(tempDir.toString())) {
            FileStoreTable table = createTable(catalog, primaryKey);
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite().withIOManager(ioManager);
                    BatchTableCommit commit = builder.newCommit()) {
                write.write(GenericRow.of(1, 10));
                write.write(GenericRow.of(2, 20));
                write.write(GenericRow.of(3, 30));
                commit.commit(write.prepareCommit());
            }
            assertThat(table.store().newScan().plan().files()).hasSize(1);
            String dataFile = table.store().newScan().plan().files().get(0).fileName();
            if (existingDv) {
                commit(
                        table,
                        prepareDelete(
                                table, table.snapshotManager().latestSnapshot(), dataFile, 2));
            }

            Snapshot base = table.snapshotManager().latestSnapshot();
            CommitMessage a = prepareDelete(table, base, dataFile, 0);
            CommitMessage b = prepareDelete(table, base, dataFile, 1);
            DataIncrement incrementA = ((CommitMessageImpl) a).newFilesIncrement();
            DataIncrement incrementB = ((CommitMessageImpl) b).newFilesIncrement();
            assertThat(incrementA.deletedIndexFiles()).hasSize(existingDv ? 1 : 0);
            assertThat(incrementB.deletedIndexFiles()).isEqualTo(incrementA.deletedIndexFiles());
            commit(table, a);
            long latest = table.snapshotManager().latestSnapshot().id();
            assertThat(table.snapshotManager().latestSnapshot().commitKind())
                    .isEqualTo(Snapshot.CommitKind.OVERWRITE);
            assertThatThrownBy(() -> commit(table, b))
                    .hasMessageContaining(
                            existingDv
                                    ? "File deletion conflicts detected"
                                    : "Deletion vector conflict");
            assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(latest);
            assertThat(readRows(table))
                    .containsExactlyInAnyOrderElementsOf(
                            existingDv
                                    ? Collections.singletonList(GenericRow.of(2, 20))
                                    : Arrays.asList(GenericRow.of(2, 20), GenericRow.of(3, 30)));

            CommitMessage recomputed =
                    prepareDelete(table, table.snapshotManager().latestSnapshot(), dataFile, 1);
            assertThat(((CommitMessageImpl) recomputed).newFilesIncrement().deletedIndexFiles())
                    .isEqualTo(incrementA.newIndexFiles());
            commit(table, recomputed);
            assertThat(readRows(table))
                    .containsExactlyInAnyOrderElementsOf(
                            existingDv
                                    ? Collections.emptyList()
                                    : Collections.singletonList(GenericRow.of(3, 30)));
        }
    }

    private FileStoreTable createTable(Catalog catalog, boolean primaryKey) throws Exception {
        catalog.createDatabase("default", true);
        Identifier identifier = Identifier.create("default", "t");
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .option("bucket", "1")
                        .option("bucket-key", "id")
                        .option("deletion-vectors.enabled", "true");
        if (primaryKey) {
            schema.primaryKey("id");
        }
        catalog.createTable(identifier, schema.build(), false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private CommitMessage prepareDelete(
            FileStoreTable table, Snapshot snapshot, String file, long position) throws Exception {
        // Use the same producer and DataIncrement layout as Spark's persistDeletionVectors.
        BaseAppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forBucketedAppend(
                        table.store().newIndexFileHandler(), snapshot, BinaryRow.EMPTY_ROW, 0);
        BitmapDeletionVector dv = new BitmapDeletionVector();
        dv.delete(position);
        maintainer.notifyNewDeletionVector(file, dv);
        List<IndexFileMeta> added = new ArrayList<>();
        List<IndexFileMeta> deleted = new ArrayList<>();
        for (IndexManifestEntry entry : maintainer.persist()) {
            (entry.kind() == FileKind.ADD ? added : deleted).add(entry.indexFile());
        }
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        1,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                added,
                                deleted),
                        CompactIncrement.emptyIncrement());
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        return serializer.deserialize(serializer.getVersion(), serializer.serialize(message));
    }

    private void commit(FileStoreTable table, CommitMessage message) throws Exception {
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private List<GenericRow> readRows(FileStoreTable table) throws Exception {
        ReadBuilder builder = table.newReadBuilder();
        List<GenericRow> result = new ArrayList<>();
        try (CloseableIterator<InternalRow> rows =
                builder.newRead().createReader(builder.newScan().plan()).toCloseableIterator()) {
            while (rows.hasNext()) {
                InternalRow row = rows.next();
                result.add(GenericRow.of(row.getInt(0), row.getInt(1)));
            }
        }
        return result;
    }
}
