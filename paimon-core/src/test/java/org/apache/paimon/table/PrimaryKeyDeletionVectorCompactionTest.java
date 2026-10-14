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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.CloseableIterator;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.CoreOptions.NUM_LEVELS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Table-level tests for primary-key compaction with deletion vectors and no clustering. */
class PrimaryKeyDeletionVectorCompactionTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentCompactionRejectsStaleDvWithDisjointKeys(boolean existingDv)
            throws Exception {
        try (Catalog catalog =
                        CatalogFactory.createCatalog(
                                CatalogContext.create(new Path(tempDir.toString())));
                IOManager ioManager = IOManager.create(tempDir.toString())) {
            catalog.createDatabase("default", true);
            Identifier identifier = Identifier.create("default", "t");
            catalog.createTable(
                    identifier,
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("v", DataTypes.INT())
                            .primaryKey("id")
                            .option(BUCKET.key(), "1")
                            .option(DELETION_VECTORS_ENABLED.key(), "true")
                            .option(NUM_LEVELS.key(), "3")
                            .build(),
                    false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);

            // A larger base file stays in L2 while each single-row update is compacted to L1.
            List<GenericRow> expected = new ArrayList<>();
            for (int id = 1; id <= 20; id++) {
                expected.add(GenericRow.of(id, id * 10));
            }
            writeRows(table, ioManager, expected);
            if (existingDv) {
                // A delete creates a real DV without leaving an L1 file shared by A and B.
                writeRows(
                        table,
                        ioManager,
                        Collections.singletonList(GenericRow.ofKind(RowKind.DELETE, 20, 200)));
                expected.remove(expected.size() - 1);
            }
            assertThat(table.store().newScan().plan().files())
                    .singleElement()
                    .satisfies(entry -> assertThat(entry.file().level()).isEqualTo(2));
            assertThat(readRows(table)).containsExactlyInAnyOrderElementsOf(expected);

            BatchWriteBuilder first = table.newBatchWriteBuilder();
            BatchWriteBuilder second = table.newBatchWriteBuilder();
            try (BatchTableWrite a = first.newWrite().withIOManager(ioManager);
                    BatchTableWrite b = second.newWrite().withIOManager(ioManager);
                    BatchTableCommit commitA = first.newCommit();
                    BatchTableCommit commitB = second.newCommit()) {
                // Prepare both results from the same snapshot before publishing either one.
                a.write(GenericRow.of(1, 100));
                b.write(GenericRow.of(2, 200));
                List<CommitMessage> preparedA = a.prepareCommit();
                List<CommitMessage> preparedB = b.prepareCommit();
                CompactIncrement incrementA = assertLookupCompaction(preparedA, 1);
                CompactIncrement incrementB = assertLookupCompaction(preparedB, 2);
                assertThat(incrementB.expectedDeletionVectorFile())
                        .isEqualTo(incrementA.expectedDeletionVectorFile());
                if (existingDv) {
                    assertThat(incrementA.expectedDeletionVectorFile()).isNotNull();
                } else {
                    assertThat(incrementA.expectedDeletionVectorFile()).isNull();
                }

                // L1 ranges [1, 1] and [2, 2] do not conflict. Only the stale DV dependency
                // prevents B from replacing A's deletion mark for id=1 in the L2 file.
                commitA.commit(preparedA);
                assertThatThrownBy(() -> commitB.commit(preparedB))
                        .hasMessageContaining("Deletion vector conflict")
                        .hasMessageContaining("recompute compaction");

                // B's APPEND may be committed, but the default DV read excludes unmerged L0.
                // A's update remains visible exactly once and B's stale compact is not visible.
                expected.set(0, GenericRow.of(1, 100));
                assertThat(readRows(table)).containsExactlyInAnyOrderElementsOf(expected);
            }
        }
    }

    private CompactIncrement assertLookupCompaction(List<CommitMessage> messages, int key) {
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).bucket()).isZero();
        CompactIncrement increment = ((CommitMessageImpl) messages.get(0)).compactIncrement();
        assertThat(increment.compactBefore())
                .singleElement()
                .satisfies(file -> assertThat(file.level()).isZero());
        assertThat(increment.compactAfter())
                .singleElement()
                .satisfies(
                        file -> {
                            assertThat(file.level()).isEqualTo(1);
                            assertThat(file.minKey().getInt(0)).isEqualTo(key);
                            assertThat(file.maxKey().getInt(0)).isEqualTo(key);
                        });
        assertThat(increment.hasDeletionVectorBaseline()).isTrue();
        assertThat(increment.newIndexFiles()).hasSize(1);
        return increment;
    }

    private void writeRows(Table table, IOManager ioManager, List<GenericRow> rows)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = builder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    private List<GenericRow> readRows(Table table) throws Exception {
        ReadBuilder builder = table.newReadBuilder();
        List<GenericRow> rows = new ArrayList<>();
        try (CloseableIterator<InternalRow> iterator =
                builder.newRead().createReader(builder.newScan().plan()).toCloseableIterator()) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                rows.add(GenericRow.of(row.getInt(0), row.getInt(1)));
            }
        }
        return rows;
    }
}
