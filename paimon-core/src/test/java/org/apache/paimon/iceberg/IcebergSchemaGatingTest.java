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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataField;
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
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for refusing schemas the Iceberg mirror cannot represent. */
public class IcebergSchemaGatingTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testCreateTableWithUnsupportedSchemaIsRejected() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.TIMESTAMP(9)},
                        new String[] {"k", "ts"});
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.FILE_FORMAT.key(), "parquet");
        assertThatThrownBy(
                        () ->
                                createPaimonTable(
                                        rowType,
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        -1,
                                        options))
                .hasStackTraceContaining("precision");
    }

    @Test
    public void testVariantColumnRejectedOnCreate() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.VARIANT()},
                        new String[] {"k", "payload"});
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.FILE_FORMAT.key(), "parquet");
        options.put(IcebergOptions.FORMAT_VERSION.key(), "3");
        assertThatThrownBy(
                        () ->
                                createPaimonTable(
                                        rowType,
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        -1,
                                        options))
                .hasStackTraceContaining("payload: VARIANT");
    }

    @Test
    public void testReplacementWithGeospatialBelowV3KeepsData() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        1,
                        Collections.singletonMap(CoreOptions.FILE_FORMAT.key(), "parquet"));

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        Map<String, String> options = new HashMap<>(table.options());
        options.remove(CoreOptions.PATH.key());
        Schema replacement =
                new Schema(
                        RowType.of(
                                        new DataType[] {DataTypes.INT(), DataTypes.GEOMETRY()},
                                        new String[] {"k", "geom"})
                                .getFields(),
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        options,
                        "");

        try (FileSystemCatalog catalog =
                new FileSystemCatalog(table.fileIO(), new Path(tempDir.toString()))) {
            Identifier identifier = Identifier.create("mydb", "t");
            assertThatThrownBy(() -> catalog.replaceTable(identifier, replacement, false))
                    .hasStackTraceContaining("format-version");
        }

        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(1L);
    }

    @Test
    public void testReplaceTableWithUnsupportedSchemaKeepsData() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType, Collections.emptyList(), Collections.singletonList("k"), 1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        Map<String, String> options = new HashMap<>(table.options());
        options.remove(CoreOptions.PATH.key());
        Schema replacement =
                new Schema(
                        RowType.of(
                                        new DataType[] {DataTypes.INT(), DataTypes.VARIANT()},
                                        new String[] {"k", "payload"})
                                .getFields(),
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        options,
                        "");

        try (FileSystemCatalog catalog =
                new FileSystemCatalog(table.fileIO(), new Path(tempDir.toString()))) {
            Identifier identifier = Identifier.create("mydb", "t");
            assertThatThrownBy(() -> catalog.replaceTable(identifier, replacement, false))
                    .hasStackTraceContaining("not supported by Iceberg compatibility");
        }

        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)");
    }

    @Test
    public void testReplaceTableDowngradingFormatVersionKeepsData() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Map<String, String> v3Options = new HashMap<>();
        v3Options.put(IcebergOptions.FORMAT_VERSION.key(), "3");
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        1,
                        v3Options);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        Map<String, String> options = new HashMap<>(table.options());
        options.remove(CoreOptions.PATH.key());
        options.put(IcebergOptions.FORMAT_VERSION.key(), "2");
        Schema downgrade =
                new Schema(
                        rowType.getFields(),
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        options,
                        "");

        try (FileSystemCatalog catalog =
                new FileSystemCatalog(table.fileIO(), new Path(tempDir.toString()))) {
            assertThatThrownBy(
                            () ->
                                    catalog.replaceTable(
                                            Identifier.create("mydb", "t"), downgrade, false))
                    .hasStackTraceContaining("cannot be lowered");
        }

        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(1L);
    }

    @Test
    public void testDropAndCreateReplacementIsRefusedBeforeTheDrop() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType, Collections.emptyList(), Collections.singletonList("k"), 1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        Map<String, String> options = new HashMap<>(table.options());
        options.remove(CoreOptions.PATH.key());
        options.put(CoreOptions.TYPE.key(), "materialized-table");
        Schema replacement =
                new Schema(
                        Arrays.asList(
                                new DataField(0, "k", DataTypes.INT()),
                                new DataField(1, "payload", DataTypes.BYTES(), "__BLOB_FIELD")),
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        options,
                        "");

        try (FileSystemCatalog catalog =
                new FileSystemCatalog(table.fileIO(), new Path(tempDir.toString()))) {
            Identifier identifier = Identifier.create("mydb", "t");
            assertThatThrownBy(() -> catalog.replaceTable(identifier, replacement, false))
                    .hasStackTraceContaining("BLOB");
        }

        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)");
    }

    @Test
    public void testReplacementKeepsUnexpandedColumnDirective() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType, Collections.emptyList(), Collections.singletonList("k"), 1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        Map<String, String> options = new HashMap<>(table.options());
        options.remove(CoreOptions.PATH.key());
        Schema replacement =
                new Schema(
                        Arrays.asList(
                                new DataField(0, "k", DataTypes.INT()),
                                new DataField(1, "payload", DataTypes.BYTES(), "__BLOB_FIELD")),
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        options,
                        "");

        try (FileSystemCatalog catalog =
                new FileSystemCatalog(table.fileIO(), new Path(tempDir.toString()))) {
            Identifier identifier = Identifier.create("mydb", "t");
            assertThatCode(() -> catalog.replaceTable(identifier, replacement, false))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    public void testEnablingMirroringIsRejectedUntilUnsupportedColumnIsGone() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        -1,
                        Collections.singletonMap(CoreOptions.FILE_FORMAT.key(), "parquet"));
        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        addUnsupportedColumnWhileMirroringOff(schemaManager, "payload", DataTypes.VARIANT());

        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        SchemaChange.setOption(
                                                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                                                "table-location")))
                .hasStackTraceContaining("VARIANT");

        schemaManager.commitChanges(SchemaChange.dropColumn("payload"));
        assertThatCode(
                        () ->
                                schemaManager.commitChanges(
                                        SchemaChange.setOption(
                                                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                                                "table-location")))
                .doesNotThrowAnyException();
    }

    @Test
    public void testSchemaIdAlreadyTakenIsReportedAsLostRace() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        -1,
                        Collections.singletonMap(CoreOptions.FILE_FORMAT.key(), "parquet"));
        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        addUnsupportedColumnWhileMirroringOff(schemaManager, "payload", DataTypes.VARIANT());

        TableSchema latest = schemaManager.latest().get();
        List<DataField> staged = new ArrayList<>(latest.fields());
        staged.add(new DataField(latest.highestFieldId() + 1, "w", DataTypes.INT()));
        Map<String, String> mirrored = new HashMap<>(latest.options());
        mirrored.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "table-location");

        assertThat(
                        schemaManager.commit(
                                new TableSchema(
                                        latest.id(),
                                        staged,
                                        latest.highestFieldId() + 1,
                                        latest.partitionKeys(),
                                        latest.primaryKeys(),
                                        mirrored,
                                        latest.comment())))
                .isFalse();
    }

    @Test
    public void testSchemaRollbackIsRefusedWhenItLowersFormatVersion() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        -1,
                        Collections.singletonMap(CoreOptions.FILE_FORMAT.key(), "parquet"));

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(
                SchemaChange.setOption(IcebergOptions.FORMAT_VERSION.key(), "3"));

        assertThatThrownBy(() -> table.copyWithLatestSchema().rollbackSchema(0))
                .hasStackTraceContaining("cannot be lowered");
        assertThat(schemaManager.latest().get().id()).isEqualTo(1L);
    }

    @Test
    public void testBlankBranchOptionStillMirrors() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(rowType, Collections.emptyList(), Collections.emptyList(), -1);

        FileStoreTable blankBranch =
                table.copy(Collections.singletonMap(CoreOptions.BRANCH.key(), ""));
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = blankBranch.newWrite(commitUser);
        TableCommitImpl commit = blankBranch.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(1L);
        assertThat(listIcebergMetadataFiles(table)).contains("v1.metadata.json");
    }

    @Test
    public void testBranchWritesAreNotVetoedAndLeaveIcebergMetadataAlone() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(rowType, Collections.emptyList(), Collections.emptyList(), -1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();
        Set<String> mirroredBefore = listIcebergMetadataFiles(table);

        table.createBranch("b1");
        FileStoreTable branchTable =
                table.copy(Collections.singletonMap(CoreOptions.BRANCH.key(), "b1"));
        new SchemaManager(table.fileIO(), table.location(), "b1")
                .commitChanges(SchemaChange.addColumn("w", DataTypes.INT()));
        branchTable = branchTable.copyWithLatestSchema();

        TableWriteImpl<?> branchWrite = branchTable.newWrite(commitUser);
        TableCommitImpl branchCommit = branchTable.newCommit(commitUser);
        branchWrite.write(GenericRow.of(2, 20, 200));
        assertThatCode(() -> branchCommit.commit(2, branchWrite.prepareCommit(false, 2)))
                .doesNotThrowAnyException();
        branchWrite.close();
        branchCommit.close();

        assertThat(listIcebergMetadataFiles(table)).isEqualTo(mirroredBefore);
    }

    @Test
    public void testBranchSchemaEvolutionIsNotGatedByIcebergRules() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        -1,
                        Collections.singletonMap(CoreOptions.FILE_FORMAT.key(), "parquet"));

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        table.createBranch("b1");
        SchemaManager branchSchemas = new SchemaManager(table.fileIO(), table.location(), "b1");
        assertThatCode(
                        () ->
                                branchSchemas.commitChanges(
                                        SchemaChange.addColumn("payload", DataTypes.VARIANT())))
                .doesNotThrowAnyException();
    }

    @Test
    public void testFastForwardIsRefusedWhenBranchSchemaCannotBeMirrored() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        -1,
                        Collections.singletonMap(CoreOptions.FILE_FORMAT.key(), "parquet"));

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        table.createBranch("b1");
        new SchemaManager(table.fileIO(), table.location(), "b1")
                .commitChanges(SchemaChange.addColumn("payload", DataTypes.VARIANT()));
        FileStoreTable branchTable =
                table.copy(Collections.singletonMap(CoreOptions.BRANCH.key(), "b1"))
                        .copyWithLatestSchema();
        TableWriteImpl<?> branchWrite = branchTable.newWrite(commitUser);
        TableCommitImpl branchCommit = branchTable.newCommit(commitUser);
        branchWrite.write(GenericRow.of(2, 20, null));
        branchCommit.commit(2, branchWrite.prepareCommit(false, 2));
        branchWrite.close();
        branchCommit.close();

        assertThatThrownBy(() -> table.fastForward("b1")).hasStackTraceContaining("VARIANT");
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(1L);
    }

    @Test
    public void testFastForwardIsRefusedWhenBranchLowersFormatVersion() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Map<String, String> v3Options = new HashMap<>();
        v3Options.put(IcebergOptions.FORMAT_VERSION.key(), "3");
        v3Options.put(CoreOptions.FILE_FORMAT.key(), "parquet");
        FileStoreTable table =
                createPaimonTable(
                        rowType, Collections.emptyList(), Collections.emptyList(), -1, v3Options);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        table.createBranch("b1");
        new SchemaManager(table.fileIO(), table.location(), "b1")
                .commitChanges(SchemaChange.setOption(IcebergOptions.FORMAT_VERSION.key(), "2"));
        FileStoreTable branchTable =
                table.copy(Collections.singletonMap(CoreOptions.BRANCH.key(), "b1"))
                        .copyWithLatestSchema();
        TableWriteImpl<?> branchWrite = branchTable.newWrite(commitUser);
        TableCommitImpl branchCommit = branchTable.newCommit(commitUser);
        branchWrite.write(GenericRow.of(2, 20));
        branchCommit.commit(2, branchWrite.prepareCommit(false, 2));
        branchWrite.close();
        branchCommit.close();

        assertThatThrownBy(() -> table.fastForward("b1"))
                .hasStackTraceContaining("cannot be lowered");
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(1L);
    }

    @Test
    public void testFastForwardCannotErasePublishedFormatVersion() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        -1,
                        Collections.singletonMap(CoreOptions.FILE_FORMAT.key(), "parquet"));

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        table.createBranch("b1");
        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(
                SchemaChange.setOption(IcebergOptions.FORMAT_VERSION.key(), "3"));
        FileStoreTable upgraded = table.copyWithLatestSchema();
        TableWriteImpl<?> v3Write = upgraded.newWrite(commitUser);
        TableCommitImpl v3Commit = upgraded.newCommit(commitUser);
        v3Write.write(GenericRow.of(2, 20));
        v3Commit.commit(2, v3Write.prepareCommit(false, 2));
        v3Write.close();
        v3Commit.close();

        SchemaManager branchSchemas = new SchemaManager(table.fileIO(), table.location(), "b1");
        branchSchemas.commitChanges(
                SchemaChange.setOption(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "disabled"));
        FileStoreTable b1 =
                table.copy(Collections.singletonMap(CoreOptions.BRANCH.key(), "b1"))
                        .copyWithLatestSchema();
        TableWriteImpl<?> b1Write = b1.newWrite(commitUser);
        TableCommitImpl b1Commit = b1.newCommit(commitUser);
        b1Write.write(GenericRow.of(3, 30));
        b1Commit.commit(3, b1Write.prepareCommit(false, 3));
        b1Write.close();
        b1Commit.close();

        assertThatThrownBy(() -> table.fastForward("b1"))
                .hasStackTraceContaining("cannot be lowered");
    }

    @Test
    public void testFastForwardBetweenBranchesIsNotGated() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        -1,
                        Collections.singletonMap(CoreOptions.FILE_FORMAT.key(), "parquet"));

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        table.createBranch("b1");
        table.createBranch("b2");
        new SchemaManager(table.fileIO(), table.location(), "b1")
                .commitChanges(SchemaChange.addColumn("payload", DataTypes.VARIANT()));
        FileStoreTable b1 =
                table.copy(Collections.singletonMap(CoreOptions.BRANCH.key(), "b1"))
                        .copyWithLatestSchema();
        TableWriteImpl<?> b1Write = b1.newWrite(commitUser);
        TableCommitImpl b1Commit = b1.newCommit(commitUser);
        b1Write.write(GenericRow.of(2, 20, null));
        b1Commit.commit(2, b1Write.prepareCommit(false, 2));
        b1Write.close();
        b1Commit.close();

        FileStoreTable b2 = table.copy(Collections.singletonMap(CoreOptions.BRANCH.key(), "b2"));
        assertThatCode(() -> b2.fastForward("b1")).doesNotThrowAnyException();
    }

    @Test
    public void testRegisteringLegacyUnmirrorableTableIsAllowed() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        -1,
                        Collections.singletonMap(CoreOptions.FILE_FORMAT.key(), "parquet"));

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        TableSchema latest = schemaManager.latest().get();
        List<DataField> legacyFields = new ArrayList<>(latest.fields());
        legacyFields.add(
                new DataField(latest.highestFieldId() + 1, "payload", DataTypes.VARIANT()));
        TableSchema legacy =
                new TableSchema(
                        latest.id() + 1,
                        legacyFields,
                        latest.highestFieldId() + 1,
                        latest.partitionKeys(),
                        latest.primaryKeys(),
                        latest.options(),
                        latest.comment());
        table.fileIO()
                .writeFile(
                        new Path(table.location(), "schema/schema-" + legacy.id()),
                        legacy.toString(),
                        true);

        Map<String, String> registration = new HashMap<>(latest.options());
        registration.put(CoreOptions.PATH.key(), table.location().toString());
        assertThatCode(
                        () ->
                                new SchemaManager(table.fileIO(), table.location())
                                        .createTable(
                                                new Schema(
                                                        Collections.emptyList(),
                                                        Collections.emptyList(),
                                                        Collections.emptyList(),
                                                        registration,
                                                        ""),
                                                true))
                .doesNotThrowAnyException();
    }

    private FileStoreTable createPaimonTable(
            RowType rowType, List<String> partitionKeys, List<String> primaryKeys, int numBuckets)
            throws Exception {
        return createPaimonTable(rowType, partitionKeys, primaryKeys, numBuckets, new HashMap<>());
    }

    private FileStoreTable createPaimonTable(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            int numBuckets,
            Map<String, String> customOptions)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        Options options = new Options(customOptions);
        options.set(CoreOptions.BUCKET, numBuckets);
        options.set(
                IcebergOptions.METADATA_ICEBERG_STORAGE, IcebergOptions.StorageType.TABLE_LOCATION);
        if (!customOptions.containsKey(CoreOptions.FILE_FORMAT.key())) {
            options.set(CoreOptions.FILE_FORMAT, "avro");
        }
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofKibiBytes(32));
        if (!customOptions.containsKey(IcebergOptions.COMPACT_MIN_FILE_NUM.key())) {
            options.set(IcebergOptions.COMPACT_MIN_FILE_NUM, 8);
        }
        options.set(IcebergOptions.METADATA_DELETE_AFTER_COMMIT, true);
        if (!customOptions.containsKey(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX.key())) {
            options.set(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX, 1);
        }
        options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE, MemorySize.ofKibiBytes(8));
        Schema schema =
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, options.toMap(), "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            return (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }
    }

    private org.apache.iceberg.Table getIcebergTable() {
        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of("mydb.db", "t");
        return icebergCatalog.loadTable(icebergIdentifier);
    }

    private List<String> getIcebergResult() throws Exception {
        return getIcebergResult(
                icebergTable -> IcebergGenerics.read(icebergTable).build(), Record::toString);
    }

    private List<String> getIcebergResult(
            Function<org.apache.iceberg.Table, CloseableIterable<Record>> query,
            Function<Record, String> icebergRecordToString)
            throws Exception {
        org.apache.iceberg.Table icebergTable = getIcebergTable();
        CloseableIterable<Record> result = query.apply(icebergTable);
        List<String> actual = new ArrayList<>();
        for (Record record : result) {
            actual.add(icebergRecordToString.apply(record));
        }
        result.close();
        return actual;
    }

    private void addUnsupportedColumnWhileMirroringOff(
            SchemaManager schemaManager, String name, DataType type) throws Exception {
        schemaManager.commitChanges(
                SchemaChange.setOption(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "disabled"));
        schemaManager.commitChanges(SchemaChange.addColumn(name, type));
    }

    private Set<String> listIcebergMetadataFiles(FileStoreTable table) throws Exception {
        return Arrays.stream(table.fileIO().listStatus(new Path(table.location(), "metadata")))
                .map(status -> status.getPath().getName())
                .collect(Collectors.toSet());
    }
}
