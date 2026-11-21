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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.errors.ErrorMessages.DATA_EVOLUTION_ROW_ID_CONFLICT_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Small sort buffers must retain ordinary Data Evolution commit semantics. */
class DataEvolutionSmallBufferTest {

    @TempDir java.nio.file.Path temporary;

    @ParameterizedTest
    @CsvSource({
        "off, 4",
        "lightweight, 4",
        "streaming, 4",
        "off, 6",
        "lightweight, 6",
        "streaming, 6"
    })
    void testPartialUpdateAndStaleConflict(String mode, int pages) throws Exception {
        FileStoreTable table;
        try (Catalog catalog =
                CatalogFactory.createCatalog(CatalogContext.create(new Path(temporary.toUri())))) {
            catalog.createDatabase("default", true);
            Identifier identifier = Identifier.create("default", "small_buffer");
            catalog.createTable(
                    identifier,
                    Schema.newBuilder()
                            .column("f0", DataTypes.INT())
                            .column("f1", DataTypes.STRING())
                            .column("f2", DataTypes.STRING())
                            .option(CoreOptions.BUCKET.key(), "-1")
                            .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                            .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                            .option(
                                    CoreOptions.DATA_EVOLUTION_METADATA_MEMORY_OPTIMIZATION_MODE
                                            .key(),
                                    mode)
                            .option(CoreOptions.PAGE_SIZE.key(), "64 kb")
                            .option(CoreOptions.SORT_SPILL_BUFFER_SIZE.key(), pages * 64 + " kb")
                            .build(),
                    false);
            table = (FileStoreTable) catalog.getTable(identifier);
        }
        BatchWriteBuilder initial = table.newBatchWriteBuilder();
        try (BatchTableWrite write = initial.newWrite();
                BatchTableCommit commit = initial.newCommit()) {
            write.write(
                    GenericRow.of(
                            1,
                            BinaryString.fromString("untouched"),
                            BinaryString.fromString("base")));
            commit.commit(write.prepareCommit());
        }
        long readSnapshot = table.snapshotManager().latestSnapshot().id();
        assertThat(readSnapshot).isEqualTo(1L);
        long firstRowId = table.snapshotManager().latestSnapshot().nextRowId() - 1;
        String originalFile = table.store().newScan().readSimpleEntries().get(0).fileName();

        BatchWriteBuilder staleBuilder = table.newBatchWriteBuilder();
        List<CommitMessage> stale =
                prepareUpdate(staleBuilder, table, firstRowId, readSnapshot, "stale");
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        List<CommitMessage> update =
                prepareUpdate(builder, table, firstRowId, readSnapshot, "updated");
        try (BatchTableCommit commit = builder.newCommit()) {
            commit.commit(update);
        }
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(2L);
        assertThat(
                        table.store().newScan().readSimpleEntries().stream()
                                .map(entry -> entry.fileName())
                                .collect(Collectors.toList()))
                .contains(originalFile);

        assertThatThrownBy(
                        () -> {
                            try (BatchTableCommit commit = staleBuilder.newCommit()) {
                                commit.commit(stale);
                            }
                        })
                .hasMessageContaining(DATA_EVOLUTION_ROW_ID_CONFLICT_MESSAGE);
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(2L);
        try (RecordReaderIterator<InternalRow> reader =
                new RecordReaderIterator<>(
                        table.newRead().createReader(table.newScan().plan().splits()))) {
            assertThat(reader.hasNext()).isTrue();
            InternalRow row = reader.next();
            assertThat(row.getInt(0)).isEqualTo(1);
            assertThat(row.getString(1).toString()).isEqualTo("untouched");
            assertThat(row.getString(2).toString()).isEqualTo("updated");
            assertThat(reader.hasNext()).isFalse();
        }
    }

    private List<CommitMessage> prepareUpdate(
            BatchWriteBuilder builder,
            FileStoreTable table,
            long firstRowId,
            long readSnapshot,
            String value)
            throws Exception {
        try (BatchTableWrite write =
                builder.newWrite()
                        .withWriteType(table.rowType().project(Collections.singletonList("f2")))) {
            write.write(GenericRow.of(BinaryString.fromString(value)));
            List<CommitMessage> messages = write.prepareCommit();
            for (CommitMessage message : messages) {
                ((CommitMessageImpl) message)
                        .newFilesIncrement()
                        .newFiles()
                        .replaceAll(file -> file.assignFirstRowId(firstRowId));
            }
            return messages.stream()
                    .map(
                            message ->
                                    ((CommitMessageImpl) message)
                                            .withCheckFromSnapshot(readSnapshot))
                    .collect(Collectors.toList());
        }
    }
}
