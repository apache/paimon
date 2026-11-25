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
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests branch identities used when chain tables load snapshots through a catalog. */
class FileStoreTableFactoryTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testChainBranchesLoadTheirOwnCatalogSnapshots(
            boolean withSnapshot, boolean legacyPartitionName) throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.CHAIN_TABLE_ENABLED, true);
        options.set(CoreOptions.SEQUENCE_FIELD, "seq");
        options.set(CoreOptions.PARTITION_TIMESTAMP_PATTERN, "$dt");
        options.set(CoreOptions.PARTITION_TIMESTAMP_FORMATTER, "yyyy-MM-dd");
        options.set(CoreOptions.PARTITION_GENERATE_LEGACY_NAME, legacyPartitionName);
        options.set(CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH, "snapshot");
        options.set(CoreOptions.SCAN_FALLBACK_DELTA_BRANCH, "delta");
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("seq", DataTypes.INT())
                        .field("dt", DataTypes.STRING())
                        .build();
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(fileIO, path),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("dt"),
                                Arrays.asList("dt", "id"),
                                options.toMap(),
                                ""));
        FileStoreTable main =
                FileStoreTableFactory.createWithoutFallbackBranch(
                        fileIO, path, schema, new Options(), CatalogEnvironment.empty());
        main.createBranch("snapshot");
        main.createBranch("delta");
        if (withSnapshot) {
            writeRow(main.switchToBranch("snapshot"), 1, LocalDate.of(2025, 1, 1));
        }
        writeRow(main.switchToBranch("delta"), 2, LocalDate.of(2025, 1, 2));
        assertThat(main.snapshotManager().latestSnapshot()).isNull();

        // A versioned catalog returns no snapshot for main, but snapshots for its branches.
        Catalog catalog = mock(Catalog.class);
        when(catalog.loadSnapshot(any(Identifier.class)))
                .thenAnswer(
                        invocation -> {
                            Identifier identifier = invocation.getArgument(0);
                            Snapshot snapshot =
                                    new SnapshotManager(
                                                    fileIO,
                                                    path,
                                                    identifier.getBranchName(),
                                                    null,
                                                    null)
                                            .latestSnapshot();
                            return Optional.ofNullable(snapshot)
                                    .map(s -> new TableSnapshot(s, 0, 0, 0, 0));
                        });
        Identifier mainIdentifier = Identifier.create("db", "events");
        CatalogEnvironment environment =
                new CatalogEnvironment(
                        mainIdentifier, "main-uuid", () -> catalog, null, null, null, true, false);
        FallbackReadFileStoreTable table =
                (FallbackReadFileStoreTable)
                        FileStoreTableFactory.create(
                                fileIO, path, schema, new Options(), environment);
        ChainGroupReadTable chain = (ChainGroupReadTable) table.other();
        List<Split> splits =
                chain.newScan()
                        .withPartitionFilter(Collections.singletonMap("dt", "2025-01-02"))
                        .plan()
                        .splits();
        assertThat(splits).as("delta data must not be treated as empty main input").isNotEmpty();
        List<Integer> ids = new ArrayList<>();
        for (Split split : splits) {
            try (RecordReader<InternalRow> reader = chain.newRead().createReader(split)) {
                reader.forEachRemaining(
                        row -> {
                            ids.add(row.getInt(0));
                            assertThat(row.getString(2).toString()).isEqualTo("2025-01-02");
                        });
            }
        }
        assertThat(ids)
                .containsExactlyInAnyOrderElementsOf(
                        withSnapshot ? Arrays.asList(1, 2) : Collections.singletonList(2));
        Identifier snapshotIdentifier = new Identifier("db", "events", "snapshot");
        Identifier deltaIdentifier = new Identifier("db", "events", "delta");
        verify(catalog, atLeastOnce()).loadSnapshot(snapshotIdentifier);
        verify(catalog, atLeastOnce()).loadSnapshot(deltaIdentifier);
        verify(catalog, never()).loadSnapshot(mainIdentifier);
        assertThat(chain.wrapped().catalogEnvironment().identifier()).isEqualTo(snapshotIdentifier);
        assertThat(chain.other().catalogEnvironment().identifier()).isEqualTo(deltaIdentifier);
        assertThat(chain.wrapped().catalogEnvironment().uuid()).isNull();
        assertThat(chain.other().catalogEnvironment().uuid()).isNull();
        assertThat(table.wrapped().catalogEnvironment().identifier()).isEqualTo(mainIdentifier);
        assertThat(table.wrapped().catalogEnvironment().uuid()).isEqualTo("main-uuid");
    }

    private void writeRow(FileStoreTable table, int id, LocalDate date) throws Exception {
        try (StreamTableWrite write = table.newWrite("test");
                StreamTableCommit commit = table.newCommit("test")) {
            write.write(GenericRow.of(id, 1, BinaryString.fromString(date.toString())));
            commit.commit(0, write.prepareCommit(false, 0));
        }
    }
}
