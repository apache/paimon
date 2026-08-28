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
import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for ordered mirroring and catch-up in the Iceberg compatibility layer. */
public class IcebergMirrorCatchUpTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testMirrorRebuildsMissingBaseInSnapshotOrder() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table = createPaimonTable(rowType);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        write.write(GenericRow.of(3, 30));
        commit.commit(2, write.prepareCommit(false, 2));

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        table.fileIO().deleteQuietly(pathFactory.toMetadataPath(2));

        write.write(GenericRow.of(4, 40));
        commit.commit(3, write.prepareCommit(false, 3));
        write.close();
        commit.close();

        IcebergMetadata metadata =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(3));
        assertThat(metadata.currentSnapshotId()).isEqualTo(3L);
        IcebergMetadata rebuiltBase =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(2));
        assertThat(rebuiltBase.currentSnapshotId()).isEqualTo(2L);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 10)", "Record(2, 20)", "Record(3, 30)", "Record(4, 40)");
    }

    @Test
    public void testRecommitAfterRollbackReplacesAbandonedTwin() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table = createPaimonTable(rowType);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        write.write(GenericRow.of(3, 30));
        write.write(GenericRow.of(4, 40));
        commit.commit(2, write.prepareCommit(false, 2));
        write.write(GenericRow.of(5, 50));
        write.write(GenericRow.of(6, 60));
        commit.commit(3, write.prepareCommit(false, 3));
        write.close();
        commit.close();

        table.rollbackTo(2);

        TableWriteImpl<?> write2 = table.newWrite(commitUser);
        TableCommitImpl commit2 = table.newCommit(commitUser);
        write2.write(GenericRow.of(7, 70));
        write2.write(GenericRow.of(8, 80));
        commit2.commit(4, write2.prepareCommit(false, 4));
        write2.close();
        commit2.close();

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(3));
        assertThat(metadata.currentSnapshot().summary().get("paimon-commit-identity"))
                .isEqualTo(table.snapshotManager().snapshot(3).uuid());
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 10)",
                        "Record(2, 20)",
                        "Record(3, 30)",
                        "Record(4, 40)",
                        "Record(7, 70)",
                        "Record(8, 80)");
    }

    @Test
    public void testCatchUpAfterRollbackSkipsAbandonedMirrorBase() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table = createPaimonTable(rowType);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        write.write(GenericRow.of(3, 30));
        commit.commit(2, write.prepareCommit(false, 2));
        write.write(GenericRow.of(4, 40));
        commit.commit(3, write.prepareCommit(false, 3));
        write.close();
        commit.close();

        table.rollbackTo(2);

        FileStoreTable disabled =
                table.copy(
                        Collections.singletonMap(
                                IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "disabled"));
        TableWriteImpl<?> disabledWrite = disabled.newWrite(commitUser);
        TableCommitImpl disabledCommit = disabled.newCommit(commitUser);
        disabledWrite.write(GenericRow.of(5, 50));
        disabledWrite.write(GenericRow.of(6, 60));
        disabledCommit.commit(4, disabledWrite.prepareCommit(false, 4));
        disabledWrite.write(GenericRow.of(7, 70));
        disabledCommit.commit(5, disabledWrite.prepareCommit(false, 5));
        disabledWrite.close();
        disabledCommit.close();

        TableWriteImpl<?> write2 = table.newWrite(commitUser);
        TableCommitImpl commit2 = table.newCommit(commitUser);
        write2.write(GenericRow.of(8, 80));
        commit2.commit(6, write2.prepareCommit(false, 6));
        write2.close();
        commit2.close();

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(5));
        assertThat(metadata.currentSnapshotId()).isEqualTo(5L);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 10)",
                        "Record(2, 20)",
                        "Record(3, 30)",
                        "Record(5, 50)",
                        "Record(6, 60)",
                        "Record(7, 70)",
                        "Record(8, 80)");
    }

    @Test
    public void testColdStartMintsMetadataUnderCatalogLock() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table = createPaimonTable(rowType);
        Path coldStartMetadata =
                new IcebergPathFactory(new Path(table.location(), "metadata")).toMetadataPath(1);
        ProbingLockFactory.reset(table.fileIO(), coldStartMetadata);
        FileStoreTable lockedTable =
                FileStoreTableFactory.create(
                        table.fileIO(),
                        table.location(),
                        table.schema(),
                        new CatalogEnvironment(
                                Identifier.create("mydb", "t"),
                                null,
                                null,
                                new ProbingLockFactory(),
                                null,
                                null,
                                false,
                                false));

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = lockedTable.newWrite(commitUser);
        TableCommitImpl commit = lockedTable.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        assertThat(table.fileIO().exists(coldStartMetadata)).isTrue();
        assertThat(ProbingLockFactory.acquisitions.get()).isGreaterThan(0);
        assertThat(ProbingLockFactory.metadataExistedAtLockEntry.get()).isFalse();
        assertThat(ProbingLockFactory.closes.get()).isEqualTo(ProbingLockFactory.creations.get());
    }

    @Test
    public void testTransientHintReadFailureFailsCommit() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table = createPaimonTable(rowType);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        FileStoreTable failing =
                FileStoreTableFactory.create(
                        new FailingReadFileIO("version-hint.text"),
                        table.location(),
                        table.schema(),
                        CatalogEnvironment.empty());
        TableWriteImpl<?> write2 = failing.newWrite(commitUser);
        TableCommitImpl commit2 = failing.newCommit(commitUser);
        write2.write(GenericRow.of(2, 20));
        assertThatThrownBy(() -> commit2.commit(2, write2.prepareCommit(false, 2)))
                .hasStackTraceContaining("injected read failure");
        write2.close();
        commit2.close();
    }

    @Test
    public void testTransientBaseReadFailureDoesNotRetireBase() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.singletonMap(
                                IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX.key(), "3"));

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.write(GenericRow.of(2, 20));
        commit.commit(2, write.prepareCommit(false, 2));
        write.close();
        commit.close();

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        String v2Before = table.fileIO().readFileUtf8(pathFactory.toMetadataPath(2));

        FileStoreTable disabled =
                table.copy(
                        Collections.singletonMap(
                                IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "disabled"));
        TableWriteImpl<?> disabledWrite = disabled.newWrite(commitUser);
        TableCommitImpl disabledCommit = disabled.newCommit(commitUser);
        disabledWrite.write(GenericRow.of(3, 30));
        disabledCommit.commit(3, disabledWrite.prepareCommit(false, 3));
        disabledWrite.close();
        disabledCommit.close();

        FileStoreTable failing =
                FileStoreTableFactory.create(
                        new FailingReadFileIO("v2.metadata.json"),
                        table.location(),
                        table.schema(),
                        CatalogEnvironment.empty());
        TableWriteImpl<?> write2 = failing.newWrite(commitUser);
        TableCommitImpl commit2 = failing.newCommit(commitUser);
        write2.write(GenericRow.of(4, 40));
        assertThatThrownBy(() -> commit2.commit(4, write2.prepareCommit(false, 4)))
                .hasStackTraceContaining("injected read failure");
        write2.close();
        commit2.close();

        assertThat(table.fileIO().readFileUtf8(pathFactory.toMetadataPath(2))).isEqualTo(v2Before);

        TableWriteImpl<?> write3 = table.newWrite(commitUser);
        TableCommitImpl commit3 = table.newCommit(commitUser);
        write3.write(GenericRow.of(5, 50));
        commit3.commit(5, write3.prepareCommit(false, 5));
        write3.close();
        commit3.close();

        long latest = table.snapshotManager().latestSnapshotId();
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(latest));
        assertThat(metadata.currentSnapshotId()).isEqualTo(latest);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 10)", "Record(2, 20)", "Record(3, 30)", "Record(5, 50)");
    }

    @Test
    public void testDroppedUnsupportedColumnDoesNotBlockCommits() throws Exception {
        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"k"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.singletonMap(CoreOptions.FILE_FORMAT.key(), "parquet"));
        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(
                SchemaChange.setOption(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "disabled"));
        schemaManager.commitChanges(SchemaChange.addColumn("ts", DataTypes.TIMESTAMP(9)));
        FileStoreTable disabled = table.copy(schemaManager.latest().get());

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = disabled.newWrite(commitUser);
        TableCommitImpl commit = disabled.newCommit(commitUser);
        write.write(GenericRow.of(1, Timestamp.fromEpochMillis(1000)));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        schemaManager.commitChanges(SchemaChange.dropColumn("ts"));
        schemaManager.commitChanges(
                SchemaChange.setOption(
                        IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "table-location"));
        FileStoreTable evolved = table.copy(schemaManager.latest().get());

        TableWriteImpl<?> write2 = evolved.newWrite(commitUser);
        TableCommitImpl commit2 = evolved.newCommit(commitUser);
        write2.write(GenericRow.of(2));
        commit2.commit(2, write2.prepareCommit(false, 2));
        write2.close();
        commit2.close();

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(2));
        assertThat(metadata.currentSnapshotId()).isEqualTo(2L);
        assertThat(metadata.schemas())
                .isNotEmpty()
                .allSatisfy(
                        schema ->
                                assertThat(schema.fields())
                                        .noneMatch(
                                                f ->
                                                        String.valueOf(f.type())
                                                                .contains("timestamp_ns")));
        assertThat(
                        metadata.schemas().stream()
                                .filter(s -> s.schemaId() == metadata.currentSchemaId())
                                .findFirst()
                                .get()
                                .fields())
                .hasSize(1);
    }

    @Test
    public void testDroppedPendingUnsupportedColumnDoesNotBlockMirroredTable() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
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
                SchemaChange.setOption(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "disabled"));
        schemaManager.commitChanges(SchemaChange.addColumn("ts", DataTypes.TIMESTAMP(9)));
        FileStoreTable withNanos =
                table.copy(schemaManager.latest().get())
                        .copy(
                                Collections.singletonMap(
                                        IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                                        "table-location"));
        TableWriteImpl<?> write2 = withNanos.newWrite(commitUser);
        TableCommitImpl commit2 = withNanos.newCommit(commitUser);
        write2.write(GenericRow.of(2, 20, Timestamp.fromEpochMillis(1000)));
        assertThatThrownBy(() -> commit2.commit(2, write2.prepareCommit(false, 2)))
                .hasStackTraceContaining("precision");
        write2.close();
        commit2.close();

        schemaManager.commitChanges(SchemaChange.dropColumn("ts"));
        schemaManager.commitChanges(
                SchemaChange.setOption(
                        IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "table-location"));
        FileStoreTable healed = table.copy(schemaManager.latest().get());
        TableWriteImpl<?> write3 = healed.newWrite(commitUser);
        TableCommitImpl commit3 = healed.newCommit(commitUser);
        write3.write(GenericRow.of(3, 30));
        commit3.commit(3, write3.prepareCommit(false, 3));
        write3.close();
        commit3.close();

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        long latest = table.snapshotManager().latestSnapshotId();
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(latest));
        assertThat(metadata.currentSnapshotId()).isEqualTo(latest);
        assertThat(metadata.schemas())
                .isNotEmpty()
                .allSatisfy(
                        schema ->
                                assertThat(schema.fields())
                                        .noneMatch(
                                                f ->
                                                        String.valueOf(f.type())
                                                                .contains("timestamp_ns")));
    }

    @Test
    public void testDelayedRetryDoesNotPublishBehindHead() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.singletonMap(
                                IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX.key(), "3"));

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.write(GenericRow.of(2, 20));
        List<CommitMessage> secondCommitMessages = write.prepareCommit(false, 2);
        commit.commit(2, secondCommitMessages);
        write.write(GenericRow.of(3, 30));
        commit.commit(3, write.prepareCommit(false, 3));
        write.close();
        commit.close();

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        Path hintPath = new Path(pathFactory.metadataDirectory(), "version-hint.text");
        table.fileIO().deleteQuietly(pathFactory.toMetadataPath(2));
        table.fileIO().deleteQuietly(hintPath);

        TableCommitImpl commit2 = table.newCommit(commitUser);
        commit2.filterAndCommit(Collections.singletonMap(2L, secondCommitMessages));
        commit2.close();

        assertThat(table.fileIO().exists(pathFactory.toMetadataPath(2))).isTrue();
        assertThat(table.fileIO().exists(hintPath)).isFalse();
    }

    @Test
    public void testExpiryToleratesAlreadyExpiredLists() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table = createPaimonTable(rowType);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.write(GenericRow.of(2, 20));
        commit.commit(2, write.prepareCommit(false, 2));
        write.close();
        commit.close();

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergMetadata v1 =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(1));
        IcebergMetadata v2 =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(2));
        String firstList = new Path(v1.currentSnapshot().manifestList()).getName();
        String secondList = new Path(v2.currentSnapshot().manifestList()).getName();
        table.fileIO().deleteQuietly(new Path(pathFactory.metadataDirectory(), firstList));

        IcebergCommitCallback callback = new IcebergCommitCallback(table, commitUser);
        assertThatCode(() -> callback.expireManifestList(firstList, secondList))
                .doesNotThrowAnyException();
        assertThatCode(() -> callback.expireManifestList(secondList, "snap-0-missing.avro"))
                .doesNotThrowAnyException();
        assertThat(table.fileIO().exists(new Path(pathFactory.metadataDirectory(), secondList)))
                .isTrue();
        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");
    }

    @Test
    public void testTransientSchemaReadFailureFailsCommitInsteadOfDroppingSchema()
            throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table = createPaimonTable(rowType);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(SchemaChange.addColumn("w", DataTypes.INT()));
        schemaManager.commitChanges(SchemaChange.addColumn("x", DataTypes.INT()));
        FileStoreTable evolved = table.copy(schemaManager.latest().get());

        TableWriteImpl<?> write2 = evolved.newWrite(commitUser);
        List<CommitMessage> messages;
        write2.write(GenericRow.of(2, 20, 21, 22));
        messages = write2.prepareCommit(false, 2);
        write2.close();

        FileStoreTable failing =
                FileStoreTableFactory.create(
                        new FailingReadFileIO("schema-1"),
                        table.location(),
                        schemaManager.latest().get(),
                        CatalogEnvironment.empty());
        TableCommitImpl commit2 = failing.newCommit(commitUser);
        assertThatThrownBy(() -> commit2.commit(2, messages))
                .hasStackTraceContaining("injected read failure");
        commit2.close();
    }

    @Test
    public void testExpiryKeepsListOnTransientReadFailure() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table = createPaimonTable(rowType);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.write(GenericRow.of(2, 20));
        commit.commit(2, write.prepareCommit(false, 2));
        write.close();
        commit.close();

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergMetadata v1 =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(1));
        IcebergMetadata v2 =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(2));
        String firstList = new Path(v1.currentSnapshot().manifestList()).getName();
        String secondList = new Path(v2.currentSnapshot().manifestList()).getName();

        FileStoreTable failing =
                FileStoreTableFactory.create(
                        new FailingReadFileIO(firstList),
                        table.location(),
                        table.schema(),
                        CatalogEnvironment.empty());
        IcebergCommitCallback callback = new IcebergCommitCallback(failing, commitUser);
        assertThatCode(() -> callback.expireManifestList(firstList, secondList))
                .doesNotThrowAnyException();
        assertThat(table.fileIO().exists(new Path(pathFactory.metadataDirectory(), firstList)))
                .isTrue();
    }

    @Test
    public void testReenableAfterRollbackAndExpiryRebuildsInsteadOfTrustingAbandonedBase()
            throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table = createPaimonTable(rowType);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.write(GenericRow.of(2, 20));
        commit.commit(2, write.prepareCommit(false, 2));
        write.write(GenericRow.of(3, 30));
        commit.commit(3, write.prepareCommit(false, 3));
        write.close();
        commit.close();

        FileStoreTable disabled =
                table.copy(
                        Collections.singletonMap(
                                IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "disabled"));
        disabled.rollbackTo(2);
        TableWriteImpl<?> disabledWrite = disabled.newWrite(commitUser);
        TableCommitImpl disabledCommit = disabled.newCommit(commitUser);
        disabledWrite.write(GenericRow.of(4, 40));
        disabledCommit.commit(4, disabledWrite.prepareCommit(false, 4));
        disabledWrite.write(GenericRow.of(5, 50));
        disabledCommit.commit(5, disabledWrite.prepareCommit(false, 5));
        disabledWrite.close();
        disabledCommit.close();

        disabled.newExpireSnapshots()
                .config(ExpireConfig.builder().snapshotRetainMax(1).snapshotRetainMin(1).build())
                .expire();
        assertThat(table.snapshotManager().snapshotExists(3)).isFalse();

        TableWriteImpl<?> write2 = table.newWrite(commitUser);
        TableCommitImpl commit2 = table.newCommit(commitUser);
        write2.write(GenericRow.of(6, 60));
        commit2.commit(6, write2.prepareCommit(false, 6));
        write2.close();
        commit2.close();

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(5));
        assertThat(metadata.currentSnapshotId()).isEqualTo(5L);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 10)",
                        "Record(2, 20)",
                        "Record(4, 40)",
                        "Record(5, 50)",
                        "Record(6, 60)");
    }

    private FileStoreTable createPaimonTable(RowType rowType) throws Exception {
        return createPaimonTable(rowType, Collections.emptyMap());
    }

    private FileStoreTable createPaimonTable(RowType rowType, Map<String, String> customOptions)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        Options options = new Options(customOptions);
        options.set(CoreOptions.BUCKET, -1);
        options.set(
                IcebergOptions.METADATA_ICEBERG_STORAGE, IcebergOptions.StorageType.TABLE_LOCATION);
        if (!customOptions.containsKey(CoreOptions.FILE_FORMAT.key())) {
            options.set(CoreOptions.FILE_FORMAT, "avro");
        }
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofKibiBytes(32));
        options.set(IcebergOptions.COMPACT_MIN_FILE_NUM, 8);
        options.set(IcebergOptions.METADATA_DELETE_AFTER_COMMIT, true);
        if (!customOptions.containsKey(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX.key())) {
            options.set(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX, 1);
        }
        options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE, MemorySize.ofKibiBytes(8));
        Schema schema =
                new Schema(
                        rowType.getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options.toMap(),
                        "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            return (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }
    }

    private List<String> getIcebergResult() throws Exception {
        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of("mydb.db", "t");
        org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(icebergIdentifier);
        CloseableIterable<Record> result = IcebergGenerics.read(icebergTable).build();
        List<String> actual = new ArrayList<>();
        for (Record record : result) {
            actual.add(record.toString());
        }
        result.close();
        return actual;
    }

    private static class FailingReadFileIO extends LocalFileIO {

        private final String failingName;

        private FailingReadFileIO(String failingName) {
            this.failingName = failingName;
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            if (path.getName().equals(failingName)) {
                throw new IOException("injected read failure for " + failingName);
            }
            return super.newInputStream(path);
        }
    }

    private static class ProbingLockFactory implements CatalogLockFactory {

        private static final AtomicInteger acquisitions = new AtomicInteger();
        private static final AtomicInteger creations = new AtomicInteger();
        private static final AtomicInteger closes = new AtomicInteger();
        private static final AtomicBoolean metadataExistedAtLockEntry = new AtomicBoolean();
        private static FileIO probeFileIO;
        private static Path probePath;

        private static void reset(FileIO fileIO, Path path) {
            acquisitions.set(0);
            creations.set(0);
            closes.set(0);
            metadataExistedAtLockEntry.set(false);
            probeFileIO = fileIO;
            probePath = path;
        }

        @Override
        public CatalogLock createLock(CatalogLockContext context) {
            creations.incrementAndGet();
            return new CatalogLock() {
                @Override
                public <T> T runWithLock(String database, String tableName, Callable<T> callable)
                        throws Exception {
                    acquisitions.incrementAndGet();
                    if (probeFileIO.exists(probePath)) {
                        metadataExistedAtLockEntry.set(true);
                    }
                    return callable.call();
                }

                @Override
                public void close() {
                    closes.incrementAndGet();
                }
            };
        }

        @Override
        public String identifier() {
            return "probing";
        }
    }
}
