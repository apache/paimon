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
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestSidecar;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for metadata loading without runtime initialization. */
public class TableRuntimeValidationTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMetadataManagersUseSharedCoreOptions(boolean primaryKey) throws Exception {
        Schema.Builder builder = Schema.newBuilder().column("id", DataTypes.INT());
        if (primaryKey) {
            builder.primaryKey("id").option("bucket", "1");
        }
        catalog.createTable(identifier(), builder.build(), false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        CoreOptions options = table.coreOptions();
        Path relocated = new Path(warehouse, "relocated");
        options.toConfiguration().set(CoreOptions.PATH, relocated.toString());
        options.toConfiguration().set(CoreOptions.BRANCH, "dev");
        options.toConfiguration().set(CoreOptions.RECORD_LEVEL_TIME_FIELD, "missing");

        SnapshotManager snapshots = table.snapshotManager();
        ChangelogManager changelogs = table.changelogManager();
        assertThat(snapshots.tablePath()).isEqualTo(relocated);
        assertThat(snapshots.branch()).isEqualTo("dev");
        assertThatThrownBy(table::store).hasMessageContaining("missing");

        options.toConfiguration().remove(CoreOptions.RECORD_LEVEL_TIME_FIELD);
        FileStore<?> store = table.store();
        assertThat(snapshots.snapshotDirectory())
                .isEqualTo(store.snapshotManager().snapshotDirectory());
        assertThat(changelogs.changelogDirectory())
                .isEqualTo(store.changelogManager().changelogDirectory());

        Path afterInitialization = new Path(warehouse, "after-initialization");
        options.toConfiguration().set(CoreOptions.PATH, afterInitialization.toString());
        options.toConfiguration().set(CoreOptions.BRANCH, "other");
        assertThat(table.snapshotManager().tablePath()).isEqualTo(afterInitialization);
        assertThat(table.snapshotManager().branch()).isEqualTo("other");
        assertThat(table.snapshotManager().snapshotDirectory())
                .isEqualTo(store.snapshotManager().snapshotDirectory());
        assertThat(table.changelogManager().changelogDirectory())
                .isEqualTo(store.changelogManager().changelogDirectory());
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    public void testWriteOnlyOverrideForSnapshotOrdering(
            boolean copyWriteOnly, boolean initializeFirst) throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.SEQUENCE_SNAPSHOT_ORDERING.key(), "true")
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .build(),
                false);
        FileStoreTable original = (FileStoreTable) catalog.getTable(identifier());
        FileStoreTable table = original;
        if (copyWriteOnly) {
            table = table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "false"));
        }
        if (initializeFirst) {
            table.store();
        }
        CoreOptions options = table.coreOptions();
        if (!copyWriteOnly) {
            options.toConfiguration().set(CoreOptions.WRITE_ONLY, false);
        }
        options.toConfiguration().set(CoreOptions.READ_BATCH_SIZE, 32);

        assertThat(table.store().options()).isSameAs(options);
        assertThat(table.store().options().writeOnly()).isFalse();
        assertThat(table.newRead()).isNotNull();
        assertThat(table.newScan()).isNotNull();

        Map<String, String> invalidOptions = new HashMap<>(table.options());
        invalidOptions.put(CoreOptions.WRITE_ONLY.key(), "false");
        FileStoreTable invalid = original.copy(original.schema().copy(invalidOptions));
        invalid.coreOptions().toConfiguration().set(CoreOptions.READ_BATCH_SIZE, 32);
        assertThatThrownBy(invalid::store).hasMessageContaining("requires write-only = true");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSharedCoreOptions(boolean primaryKey) throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(CoreOptions.READ_BATCH_SIZE.key(), "1024");
        if (primaryKey) {
            builder.primaryKey("id").option("bucket", "1");
        }
        catalog.createTable(identifier(), builder.build(), false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        CoreOptions options = table.coreOptions();
        assertThat(table.coreOptions()).isSameAs(options);

        options.toConfiguration().set(CoreOptions.READ_BATCH_SIZE, 32);
        assertThat(table.coreOptions().toConfiguration().get(CoreOptions.READ_BATCH_SIZE))
                .isEqualTo(32);
        assertThat(table.store().options()).isSameAs(options);
        assertThat(table.store().options().toConfiguration().get(CoreOptions.READ_BATCH_SIZE))
                .isEqualTo(32);

        table.store().options().toConfiguration().set(CoreOptions.READ_BATCH_SIZE, 64);
        assertThat(table.coreOptions().toConfiguration().get(CoreOptions.READ_BATCH_SIZE))
                .isEqualTo(64);
        assertThat(table.options()).containsEntry(CoreOptions.READ_BATCH_SIZE.key(), "1024");

        FileStoreTable copied =
                table.copy(Collections.singletonMap(CoreOptions.READ_BATCH_SIZE.key(), "128"));
        assertThat(copied.coreOptions()).isNotSameAs(options);
        assertThat(copied.store().options()).isSameAs(copied.coreOptions());
        assertThat(copied.coreOptions().toConfiguration().get(CoreOptions.READ_BATCH_SIZE))
                .isEqualTo(128);
        assertThat(table.coreOptions().toConfiguration().get(CoreOptions.READ_BATCH_SIZE))
                .isEqualTo(64);

        FileStoreTable restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(table), getClass().getClassLoader());
        assertThat(restored.coreOptions()).isNotSameAs(options);
        assertThat(restored.coreOptions().toConfiguration().get(CoreOptions.READ_BATCH_SIZE))
                .isEqualTo(1024);
        assertThat(restored.store().options()).isSameAs(restored.coreOptions());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMutableCoreOptionsAreValidatedBeforeStoreInitialization(boolean primaryKey)
            throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("seq", DataTypes.INT())
                        .column("v", DataTypes.INT());
        if (primaryKey) {
            builder.primaryKey("id").option("bucket", "1").option("merge-engine", "partial-update");
        }
        catalog.createTable(identifier(), builder.build(), false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        CoreOptions options = table.coreOptions();
        options.toConfiguration().set(CoreOptions.RECORD_LEVEL_TIME_FIELD, "missing");
        assertThat(table.rowType().getFieldNames()).containsExactly("id", "seq", "v");
        assertThatThrownBy(table::store).hasMessageContaining("missing");
        options.toConfiguration().remove(CoreOptions.RECORD_LEVEL_TIME_FIELD.key());

        if (primaryKey) {
            options.toConfiguration().setString("fields.v.aggregate-function", "max");
            assertThatThrownBy(table::newRead).hasMessageContaining("Must use sequence group");
            options.toConfiguration().setString("fields.seq.sequence-group", "v");
        }
        assertThat(table.store().options()).isSameAs(options);
        write(table, GenericRow.of(1, 10, 100));
        assertThat(read(table)).extracting(row -> row.getInt(2)).containsExactly(100);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true"})
    public void testAllRuntimeEntrypointsValidate(boolean primaryKey, boolean dataEvolution)
            throws Exception {
        Schema.Builder builder = Schema.newBuilder().column("id", DataTypes.INT());
        if (primaryKey) {
            builder.primaryKey("id").option("bucket", "1");
        }
        if (dataEvolution) {
            builder.option("data-evolution.enabled", "true")
                    .option("row-tracking.enabled", "true")
                    .option("bucket", "-1");
        }
        catalog.createTable(identifier(), builder.build(), false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        table.store();
        FileStoreTable invalid =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), "missing"));
        assertThat(invalid.rowType().getFieldNames()).containsExactly("id");
        assertThat(invalid.coreOptions().recordLevelTimeField()).isEqualTo("missing");
        assertThat(invalid.snapshotManager().latestSnapshot()).isNull();
        assertThat(invalid.tagManager().allTagNames()).isEmpty();
        assertThatThrownBy(invalid::store).hasMessageContaining("missing");
        assertThatThrownBy(invalid::newRead).hasMessageContaining("missing");
        assertThatThrownBy(invalid::newScan).hasMessageContaining("missing");
        assertThatThrownBy(() -> invalid.newScan(t -> null)).hasMessageContaining("missing");
        assertThatThrownBy(invalid::newStreamScan).hasMessageContaining("missing");
        assertThatThrownBy(() -> invalid.newWrite("test")).hasMessageContaining("missing");
        assertThatThrownBy(() -> invalid.newCommit("test")).hasMessageContaining("missing");
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        identifier("invalid"),
                                        builder.option("record-level.time-field", "missing")
                                                .build(),
                                        false))
                .hasMessageContaining("missing");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCachesBeforeAndAfterStoreInitialization(boolean primaryKey) throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(CoreOptions.MANIFEST_SIDECAR_ENABLED.key(), "true");
        if (primaryKey) {
            builder.primaryKey("id").option("bucket", "1");
        }
        catalog.createTable(identifier(), builder.build(), false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        for (int i = 0; i < 2; i++) {
            SegmentsCache<Path> manifests =
                    new SegmentsCache<>(1024, new MemorySize(1024 * 1024), 1024 * 1024);
            SegmentsCache<Path> sidecars =
                    new SegmentsCache<>(1024, new MemorySize(1024 * 1024), 1024 * 1024);
            Cache<Path, Snapshot> snapshots = Caffeine.newBuilder().build();
            table.setManifestCache(manifests);
            table.setManifestSidecarCache(sidecars);
            table.setSnapshotCache(snapshots);
            write(table, GenericRow.of(i));
            assertThat(read(table)).hasSize(i + 1);
            assertThat(manifests.estimatedSize()).isPositive();
            FileStore<?> store = table.store();
            Snapshot snapshot = table.latestSnapshot().get();
            snapshots.invalidateAll();
            store.snapshotManager().snapshot(snapshot.id());
            assertThat(snapshots.getIfPresent(store.snapshotManager().snapshotPath(snapshot.id())))
                    .isNotNull();
            ManifestFileMeta manifest =
                    store.manifestListFactory().create().readDataManifests(snapshot).get(0);
            assertThat(
                            store.manifestFileFactory()
                                    .create()
                                    .selectBlocks(
                                            manifest, null, PartitionPredicate.ALWAYS_TRUE, null))
                    .isNotNull();
            assertThat(
                            sidecars.getIfPresents(
                                    new Path(
                                            table.location(),
                                            "manifest/" + ManifestSidecar.fileName(manifest))))
                    .isNotNull();
            FileStoreTable copied = table.copy(Collections.singletonMap("read.batch-size", "32"));
            assertThat(copied.getManifestCache()).isSameAs(manifests);
            assertThat(copied.getManifestSidecarCache()).isSameAs(sidecars);
            snapshots.invalidateAll();
            copied.store().snapshotManager().snapshot(snapshot.id());
            assertThat(snapshots.getIfPresent(copied.snapshotManager().snapshotPath(snapshot.id())))
                    .isNotNull();
        }
    }

    @ParameterizedTest
    @CsvSource({
        "false,-1,false,BUCKET_UNAWARE",
        "false,2,false,HASH_FIXED",
        "true,2,false,HASH_FIXED",
        "true,-1,false,HASH_DYNAMIC",
        "true,-1,true,KEY_DYNAMIC",
        "true,-2,false,POSTPONE_MODE"
    })
    public void testBucketMode(
            boolean primaryKey, int buckets, boolean crossPartition, BucketMode expected)
            throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .option("bucket", String.valueOf(buckets));
        if (primaryKey) {
            builder.primaryKey("id");
        } else if (buckets > 0) {
            builder.option("bucket-key", "id");
        }
        if (crossPartition) {
            builder.partitionKeys("pt");
        }
        catalog.createTable(identifier(), builder.build(), false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        assertThat(table.bucketMode()).isEqualTo(expected);
        assertThat(table.store().bucketMode()).isEqualTo(expected);
    }

    @Test
    public void testCopyWithoutTimeTravelDefersValidation() throws Exception {
        catalog.createTable(
                identifier(), Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
        FileStoreTable original = (FileStoreTable) catalog.getTable(identifier());
        FileSystemSchemaManager schemas =
                new FileSystemSchemaManager(original.fileIO(), original.location());
        Map<String, String> options = new HashMap<>(original.options());
        options.put(CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), "missing");
        original.fileIO()
                .overwriteFileUtf8(
                        schemas.toSchemaPath(0), original.schema().copy(options).toString());
        catalog.invalidateTable(identifier());

        FileStoreTable planning = (FileStoreTable) catalog.getTable(identifier());
        FileStoreTable copy =
                planning.copyWithoutTimeTravel(Collections.singletonMap("read.batch-size", "32"));
        assertThat(copy.options()).containsEntry("read.batch-size", "32");
        assertThatThrownBy(copy::newScan).hasMessageContaining("missing");
        FileStoreTable repaired =
                planning.copyWithoutTimeTravel(
                        Collections.singletonMap(CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), null));
        assertThat(read(repaired)).isEmpty();
    }

    @Test
    public void testStoreInitializationFailureDoesNotBlockMetadata() throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .option("merge-engine", "partial-update")
                        .build(),
                false);
        FileStoreTable original = (FileStoreTable) catalog.getTable(identifier());
        FileSystemSchemaManager schemas =
                new FileSystemSchemaManager(original.fileIO(), original.location());
        Map<String, String> options = new HashMap<>(original.options());
        options.put("fields.v.aggregate-function", "max");
        original.fileIO()
                .overwriteFileUtf8(
                        schemas.toSchemaPath(0), original.schema().copy(options).toString());
        catalog.invalidateTable(identifier());

        FileStoreTable planning = (FileStoreTable) catalog.getTable(identifier());
        assertThat(planning.options()).containsEntry("fields.v.aggregate-function", "max");
        assertThat(planning.rowType().getFieldNames()).containsExactly("id", "v");
        assertThatThrownBy(planning::newScan).hasMessageContaining("Must use sequence group");
        assertThatThrownBy(() -> planning.newWrite("test"))
                .hasMessageContaining("Must use sequence group");
        catalog.alterTable(
                identifier(), SchemaChange.removeOption("fields.v.aggregate-function"), false);
        FileStoreTable repaired = (FileStoreTable) catalog.getTable(identifier());
        write(repaired, GenericRow.of(1, 10));
        assertThat(read(repaired)).extracting(row -> row.getInt(1)).containsExactly(10);
    }

    @ParameterizedTest
    @ValueSource(strings = {"scan.fallback-branch", "scan.primary-branch"})
    public void testFallbackBranchLoading(String option) throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .partitionKeys("pt")
                        .build(),
                false);
        FileStoreTable main = (FileStoreTable) catalog.getTable(identifier());
        main.createBranch("backup");
        write(main, GenericRow.of(1, 10));
        write(main.switchToBranch("backup"), GenericRow.of(1, 20), GenericRow.of(2, 30));
        catalog.alterTable(identifier(), SchemaChange.setOption(option, "backup"), false);

        FileStoreTable planning = (FileStoreTable) catalog.getTable(identifier());
        assertThat(planning).isInstanceOf(FallbackReadFileStoreTable.class);
        assertThat(read(planning))
                .extracting(row -> row.getInt(1))
                .containsExactlyInAnyOrder(option.equals("scan.fallback-branch") ? 10 : 20, 30);
        FileStoreTable invalid =
                planning.copy(
                        Collections.singletonMap(
                                CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), "missing"));
        assertThat(invalid.rowType()).isEqualTo(planning.rowType());
        assertThatThrownBy(invalid::newRead).hasMessageContaining("missing");
        assertThatThrownBy(invalid::newScan).hasMessageContaining("missing");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCapturedSchemaAndSerialization(boolean primaryKey) throws Exception {
        Schema.Builder builder = Schema.newBuilder().column("id", DataTypes.INT());
        if (primaryKey) {
            builder.primaryKey("id").option("bucket", "1");
        }
        catalog.createTable(identifier(), builder.build(), false);
        FileStoreTable planning = (FileStoreTable) catalog.getTable(identifier());
        FileStoreTable runtime = (FileStoreTable) catalog.getTable(identifier());
        assertThat(planning.bucketMode()).isEqualTo(runtime.bucketMode());
        write(runtime, GenericRow.of(1));
        catalog.alterTable(identifier(), SchemaChange.addColumn("v", DataTypes.INT()), false);

        FileStoreTable restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(planning), getClass().getClassLoader());
        assertThat(restored.schema().id()).isEqualTo(0L);
        assertThat(restored.rowType().getFieldNames()).containsExactly("id");
        assertThat(read(restored)).extracting(row -> row.getInt(0)).containsExactly(1);
        assertThat(restored.schema()).isEqualTo(planning.schema());
    }

    @Test
    public void testTimeTravelSchemaAndDynamicOptions() throws Exception {
        catalog.createTable(
                identifier(), Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        write(table, GenericRow.of(1));
        table.createTag("before_evolution", 1);
        catalog.alterTable(identifier(), SchemaChange.addColumn("v", DataTypes.INT()), false);
        table = (FileStoreTable) catalog.getTable(identifier());
        write(table, GenericRow.of(2, 20));

        FileStoreTable planning = (FileStoreTable) catalog.getTable(identifier());
        FileStoreTable historical =
                planning.copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1"));
        historical =
                historical.copy(Collections.singletonMap(CoreOptions.READ_BATCH_SIZE.key(), "32"));
        assertThat(historical.rowType().getFieldNames()).containsExactly("id");
        assertThat(historical.options()).containsEntry(CoreOptions.READ_BATCH_SIZE.key(), "32");
        assertThat(read(historical)).extracting(row -> row.getInt(0)).containsExactly(1);
        assertThat(historical.store().options().toMap()).isEqualTo(historical.options());

        FileStoreTable tagged =
                planning.copy(
                        Collections.singletonMap(
                                CoreOptions.SCAN_TAG_NAME.key(), "before_evolution"));
        assertThat(tagged.schema()).isEqualTo(historical.schema().copy(tagged.options()));
        assertThat(read(tagged)).extracting(row -> row.getInt(0)).containsExactly(1);
        FileStoreTable noTimeTravel =
                planning.copyWithoutTimeTravel(
                        Collections.singletonMap(
                                CoreOptions.SCAN_TAG_NAME.key(), "before_evolution"));
        assertThat(noTimeTravel.rowType().getFieldNames()).containsExactly("id", "v");
        assertThat(read(noTimeTravel)).extracting(row -> row.getInt(0)).containsExactly(1);

        assertThatThrownBy(() -> planning.copy(Collections.singletonMap("bucket", "2")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(
                        () ->
                                ((FileStoreTable) catalog.getTable(identifier()))
                                        .copy(
                                                Collections.singletonMap(
                                                        CoreOptions.RECORD_LEVEL_TIME_FIELD.key(),
                                                        "missing"))
                                        .newScan())
                .isInstanceOf(IllegalArgumentException.class);
    }
}
