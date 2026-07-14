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

package org.apache.paimon.clone;

import org.apache.paimon.Changelog;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ExternalPathStrategy;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FullHistoryMetadataRewriter}. */
public class FullHistoryMetadataRewriterTest {

    @TempDir private java.nio.file.Path tempDir;

    private final FileIO fileIO = LocalFileIO.create();

    @Test
    public void testSourceFingerprintChangesAfterCommit() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("fingerprint-source/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve("fingerprint-source-external").toUri()).toString();
        FileStoreTable source = createTable(sourceRoot, sourceExternal);

        String before = FullHistorySourceFingerprint.compute(source);
        writeRows(source, 0, "A", 1);
        String after = FullHistorySourceFingerprint.compute(source);

        assertThat(after).isNotEqualTo(before);
        assertThat(FullHistorySourceFingerprint.compute(source)).isEqualTo(after);
    }

    @Test
    public void testSourceFingerprintChangesAfterSchemaChange() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("schema-fingerprint-source/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve("schema-fingerprint-external").toUri()).toString();
        FileStoreTable source = createTable(sourceRoot, sourceExternal);

        String before = FullHistorySourceFingerprint.compute(source);
        source.schemaManager().commitChanges(SchemaChange.setOption("fingerprint-test", "changed"));
        source = FileStoreTableFactory.create(fileIO, sourceRoot);

        assertThat(FullHistorySourceFingerprint.compute(source)).isNotEqualTo(before);
    }

    @Test
    public void testSourceFingerprintChangesAfterTagCreation() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("tag-fingerprint-source/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve("tag-fingerprint-external").toUri()).toString();
        FileStoreTable source = createTable(sourceRoot, sourceExternal);
        writeRows(source, 0, "A", 1);

        String before = FullHistorySourceFingerprint.compute(source);
        source.createTag("fingerprint-tag", 1);

        assertThat(FullHistorySourceFingerprint.compute(source)).isNotEqualTo(before);
    }

    @Test
    public void testSourceFingerprintChangesAfterBranchCreation() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("branch-fingerprint-source/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve("branch-fingerprint-external").toUri()).toString();
        FileStoreTable source = createTable(sourceRoot, sourceExternal);
        writeRows(source, 0, "A", 1);
        source.createTag("branch-source-tag", 1);

        String before = FullHistorySourceFingerprint.compute(source);
        source.createBranch("fingerprint-branch", "branch-source-tag");

        assertThat(FullHistorySourceFingerprint.compute(source)).isNotEqualTo(before);
    }

    @Test
    public void testSourceFingerprintChangesAfterLongLivedChangelogCreation() throws Exception {
        Path sourceRoot =
                new Path(tempDir.resolve("changelog-fingerprint-source/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve("changelog-fingerprint-external").toUri()).toString();
        FileStoreTable source = createTable(sourceRoot, sourceExternal);
        writeRows(source, 0, "A", 1);

        String before = FullHistorySourceFingerprint.compute(source);
        source.changelogManager()
                .commitChangelog(new Changelog(source.snapshotManager().latestSnapshot()), 100);

        assertThat(FullHistorySourceFingerprint.compute(source)).isNotEqualTo(before);
    }

    @Test
    public void testVerifySourceFingerprintRejectsChanges() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("verify-fingerprint-source/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve("verify-fingerprint-external").toUri()).toString();
        FileStoreTable source = createTable(sourceRoot, sourceExternal);
        String expected = FullHistorySourceFingerprint.compute(source);
        source.schemaManager().commitChanges(SchemaChange.setOption("changed", "true"));
        source = FileStoreTableFactory.create(fileIO, sourceRoot);
        FileStoreTable changedSource = source;

        assertThatThrownBy(() -> FullHistorySourceFingerprint.verify(changedSource, expected))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("changed during full-history clone");
    }

    @Test
    public void testRewriteAllHistoryWithExternalDataPaths() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("target/table").toString());
        String sourceExternal = new Path(tempDir.resolve("source-external").toUri()).toString();
        String targetExternal = new Path(tempDir.resolve("target-external").toUri()).toString();
        FileStoreTable source = createTable(sourceRoot, sourceExternal);

        writeRows(source, 0, "A", 1);
        source.createTag("tag1", 1);
        java.time.LocalDateTime sourceTagCreateTime =
                DateTimeUtils.toLocalDateTime(
                        fileIO.getFileStatus(source.tagManager().tagPath("tag1"))
                                .getModificationTime());
        source.createBranch("branch1", "tag1");
        writeRows(source.switchToBranch("branch1"), 1, "B", 2);
        source.switchToBranch("branch1")
                .schemaManager()
                .commitChanges(
                        SchemaChange.setOption(CoreOptions.SCAN_FALLBACK_BRANCH.key(), "main"));
        source.schemaManager().commitChanges(SchemaChange.setOption("clone-test", "v1"));
        source = FileStoreTableFactory.create(fileIO, sourceRoot);
        writeRows(source, 1, "C", 3);

        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceExternal + "=" + targetExternal));
        FullHistoryFileSet sourceFiles = new FullHistoryFileCollector(source).collect();
        FullHistoryCopyPlan payloadPlan =
                FullHistoryCopyPlan.buildPayload(sourceFiles, mapping, fileIO);
        FullHistoryFileCopier.copy(fileIO, fileIO, payloadPlan, false);

        new FullHistoryMetadataRewriter(source, fileIO, targetRoot, mapping).rewrite();

        FileStoreTable target = FileStoreTableFactory.create(fileIO, targetRoot);
        FullHistoryCloneValidator.ValidationResult validation =
                new FullHistoryCloneValidator(source, target, mapping, payloadPlan).validate();
        assertThat(validation.payloadFileCount()).isEqualTo(3);
        assertThat(validation.payloadBytes()).isPositive();
        assertThat(validation.metadataFileCount()).isPositive();
        assertThat(target.schemaManager().listAllIds()).containsExactly(0L, 1L);
        assertThat(target.snapshotManager().safelyGetAllSnapshots())
                .extracting(snapshot -> snapshot.id())
                .containsExactlyInAnyOrder(1L, 2L);
        assertThat(target.tagManager().tagObjects())
                .extracting(tag -> tag.getRight())
                .containsExactly("tag1");
        assertThat(target.tagManager().tagObjects().get(0).getLeft().getTagCreateTime())
                .isEqualTo(sourceTagCreateTime);
        assertThat(target.branchManager().branches()).containsExactly("branch1");
        assertThat(target.switchToBranch("branch1").snapshotManager().safelyGetAllSnapshots())
                .extracting(snapshot -> snapshot.id())
                .containsExactlyInAnyOrder(1L, 2L);
        assertThat(
                        target.switchToBranch("branch1")
                                .schemaManager()
                                .latestOrThrow("Missing branch schema.")
                                .options())
                .containsEntry(CoreOptions.SCAN_FALLBACK_BRANCH.key(), "main");

        Set<Path> targetDataFiles = new FullHistoryFileCollector(target).collect().dataFiles();
        assertThat(targetDataFiles).hasSize(3);
        assertThat(targetDataFiles).allMatch(path -> path.toString().startsWith(targetExternal));
        assertThat(targetDataFiles).allMatch(this::exists);
        assertThat(
                        target.copy(
                                        Collections.singletonMap(
                                                CoreOptions.SCAN_SNAPSHOT_ID.key(), "1"))
                                .newScan()
                                .plan()
                                .splits())
                .isNotEmpty();

        Set<String> sourceNames =
                sourceFiles.dataFiles().stream()
                        .map(path -> path.getName())
                        .collect(Collectors.toSet());
        assertThat(targetDataFiles)
                .extracting(Path::getName)
                .containsExactlyInAnyOrderElementsOf(sourceNames);
    }

    @Test
    public void testPlannerRejectsBlobDescriptorField() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("blob-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("blob-target/table").toString());
        Schema.Builder builder = Schema.newBuilder();
        builder.column("id", DataTypes.INT());
        builder.column("blob", DataTypes.BLOB());
        builder.option(CoreOptions.PATH.key(), sourceRoot.toString());
        builder.option(CoreOptions.BUCKET.key(), "-1");
        builder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        builder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        builder.option(CoreOptions.BLOB_FIELD.key(), "blob");
        builder.option(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "blob");
        TableSchema schema =
                SchemaUtils.forceCommit(new SchemaManager(fileIO, sourceRoot), builder.build());
        FileStoreTable source = FileStoreTableFactory.create(fileIO, sourceRoot, schema);

        assertThatThrownBy(
                        () ->
                                new FullHistoryClonePlanner(
                                                source,
                                                PathMapping.parse(
                                                        Collections.singletonList(
                                                                sourceRoot + "=" + targetRoot)))
                                        .plan())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(CoreOptions.BLOB_DESCRIPTOR_FIELD.key())
                .hasMessageContaining("inside data files");
    }

    @Test
    public void testPlannerRejectsMissingExternalPathMapping() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("mapping-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("mapping-target/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve("mapping-source-external").toUri()).toString();
        FileStoreTable source = createTable(sourceRoot, sourceExternal);

        assertThatThrownBy(
                        () ->
                                new FullHistoryClonePlanner(
                                                source,
                                                PathMapping.parse(
                                                        Collections.singletonList(
                                                                sourceRoot + "=" + targetRoot)))
                                        .planStructure())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No path mapping")
                .hasMessageContaining(sourceExternal);
    }

    @Test
    public void testRewriteExternalGlobalIndexPath() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("index-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("index-target/table").toString());
        String sourceIndexRoot =
                new Path(tempDir.resolve("index-source-external").toUri()).toString();
        String targetIndexRoot =
                new Path(tempDir.resolve("index-target-external").toUri()).toString();
        FileStoreTable source = createUnpartitionedTable(sourceRoot, sourceIndexRoot);
        writeRows(source, 0, 1);

        String indexFileName = "index-test";
        Path sourceIndexPath = new Path(sourceIndexRoot, indexFileName);
        fileIO.writeFile(sourceIndexPath, "index-content", false);
        GlobalIndexMeta globalIndexMeta = new GlobalIndexMeta(0, 0, 0, null, new byte[] {1, 2, 3});
        IndexManifestEntry sourceIndexEntry =
                new IndexManifestEntry(
                        FileKind.ADD,
                        BinaryRow.EMPTY_ROW,
                        0,
                        new IndexFileMeta(
                                "test-global-index",
                                indexFileName,
                                fileIO.getFileSize(sourceIndexPath),
                                1,
                                globalIndexMeta,
                                sourceIndexPath.toString()));
        String indexManifest =
                source.store()
                        .indexManifestFileFactory()
                        .create()
                        .writeWithoutRolling(Collections.singletonList(sourceIndexEntry));
        Snapshot snapshot = source.snapshotManager().latestSnapshot();
        Snapshot snapshotWithIndex = copyWithIndexManifest(snapshot, indexManifest);
        fileIO.overwriteFileUtf8(
                source.snapshotManager().snapshotPath(snapshot.id()), snapshotWithIndex.toJson());
        source = FileStoreTableFactory.create(fileIO, sourceRoot);

        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceIndexRoot + "=" + targetIndexRoot));
        FullHistoryFileSet sourceFiles = new FullHistoryFileCollector(source).collect();
        FullHistoryCopyPlan payloadPlan =
                FullHistoryCopyPlan.buildPayload(sourceFiles, mapping, fileIO);
        FullHistoryFileCopier.copy(fileIO, fileIO, payloadPlan, false);
        new FullHistoryMetadataRewriter(source, fileIO, targetRoot, mapping).rewrite();

        FileStoreTable target = FileStoreTableFactory.create(fileIO, targetRoot);
        new FullHistoryCloneValidator(source, target, mapping, payloadPlan).validate();
        IndexManifestEntry targetIndexEntry =
                target.store()
                        .newIndexFileHandler()
                        .readManifest(target.snapshotManager().latestSnapshot().indexManifest())
                        .get(0);
        assertThat(targetIndexEntry.indexFile().externalPath())
                .isEqualTo(new Path(targetIndexRoot, indexFileName).toString());
        assertThat(targetIndexEntry.indexFile().globalIndexMeta().indexMeta())
                .containsExactly(1, 2, 3);
        assertThat(fileIO.readFileUtf8(new Path(targetIndexRoot, indexFileName)))
                .isEqualTo("index-content");
    }

    @Test
    public void testRewriteLongLivedChangelogWithDedicatedManifest() throws Exception {
        assertRewriteLongLivedChangelogs(true);
    }

    @Test
    public void testRewriteLongLivedChangelogWithDeltaFallback() throws Exception {
        assertRewriteLongLivedChangelogs(false);
    }

    private void assertRewriteLongLivedChangelogs(boolean dedicatedManifest) throws Exception {
        String suffix = dedicatedManifest ? "dedicated" : "fallback";
        Path sourceRoot = new Path(tempDir.resolve(suffix + "-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve(suffix + "-target/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve(suffix + "-source-external").toUri()).toString();
        String targetExternal =
                new Path(tempDir.resolve(suffix + "-target-external").toUri()).toString();
        Options options = new Options();
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 1);
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 1);
        options.set(CoreOptions.CHANGELOG_NUM_RETAINED_MIN, 1);
        options.set(CoreOptions.CHANGELOG_NUM_RETAINED_MAX, 10);
        if (dedicatedManifest) {
            options.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.INPUT);
        }
        FileStoreTable source = createTable(sourceRoot, sourceExternal, options);
        for (int i = 1; i <= 4; i++) {
            writeRows(source, i, "A", i);
        }
        source.newExpireSnapshots().config(source.coreOptions().expireConfig()).expire();

        assertThat(source.snapshotManager().safelyGetAllSnapshots()).hasSize(1);
        assertThat(source.changelogManager().safelyGetAllChangelogs()).isNotEmpty();
        assertThat(source.changelogManager().safelyGetAllChangelogs())
                .allMatch(
                        changelog ->
                                dedicatedManifest == (changelog.changelogManifestList() != null));

        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceExternal + "=" + targetExternal));
        FullHistoryFileSet sourceFiles = new FullHistoryFileCollector(source).collect();
        assertThat(sourceFiles.allFiles()).allMatch(this::exists);
        FullHistoryCopyPlan payloadPlan =
                FullHistoryCopyPlan.buildPayload(sourceFiles, mapping, fileIO);
        FullHistoryFileCopier.copy(fileIO, fileIO, payloadPlan, false);
        new FullHistoryMetadataRewriter(source, fileIO, targetRoot, mapping).rewrite();

        FileStoreTable target = FileStoreTableFactory.create(fileIO, targetRoot);
        new FullHistoryCloneValidator(source, target, mapping, payloadPlan).validate();
        assertThat(target.changelogManager().safelyGetAllChangelogs())
                .extracting(Changelog::id)
                .containsExactlyInAnyOrderElementsOf(
                        source.changelogManager().safelyGetAllChangelogs().stream()
                                .map(Changelog::id)
                                .collect(Collectors.toList()));
        assertThat(new FullHistoryFileCollector(target).collect().allFiles())
                .allMatch(this::exists);
    }

    private FileStoreTable createTable(Path tableRoot, String externalRoot) throws Exception {
        return createTable(tableRoot, externalRoot, new Options());
    }

    private FileStoreTable createTable(Path tableRoot, String externalRoot, Options options)
            throws Exception {
        options.set(CoreOptions.PATH, tableRoot.toString());
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.BUCKET_KEY, "id");
        options.set(CoreOptions.DATA_FILE_EXTERNAL_PATHS, externalRoot);
        options.set(
                CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY, ExternalPathStrategy.ROUND_ROBIN);
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "pt"});
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, tableRoot),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                options.get(CoreOptions.CHANGELOG_PRODUCER)
                                                == CoreOptions.ChangelogProducer.NONE
                                        ? Collections.emptyList()
                                        : Arrays.asList("id", "pt"),
                                options.toMap(),
                                ""));
        return FileStoreTableFactory.create(fileIO, tableRoot, schema);
    }

    private FileStoreTable createUnpartitionedTable(Path tableRoot, String indexExternalRoot)
            throws Exception {
        Options options = new Options();
        options.set(CoreOptions.PATH, tableRoot.toString());
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.BUCKET_KEY, "id");
        options.set(CoreOptions.GLOBAL_INDEX_EXTERNAL_PATH, indexExternalRoot);
        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"id"});
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, tableRoot),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        return FileStoreTableFactory.create(fileIO, tableRoot, schema);
    }

    private static Snapshot copyWithIndexManifest(Snapshot snapshot, String indexManifest) {
        return new Snapshot(
                snapshot.version(),
                snapshot.id(),
                snapshot.schemaId(),
                snapshot.baseManifestList(),
                snapshot.baseManifestListSize(),
                snapshot.deltaManifestList(),
                snapshot.deltaManifestListSize(),
                snapshot.changelogManifestList(),
                snapshot.changelogManifestListSize(),
                indexManifest,
                snapshot.commitUser(),
                snapshot.commitIdentifier(),
                snapshot.commitKind(),
                snapshot.timeMillis(),
                snapshot.totalRecordCount(),
                snapshot.deltaRecordCount(),
                snapshot.changelogRecordCount(),
                snapshot.watermark(),
                snapshot.statistics(),
                snapshot.properties(),
                snapshot.nextRowId(),
                snapshot.operation());
    }

    private void writeRows(
            FileStoreTable table, long commitIdentifier, String partition, int... ids)
            throws Exception {
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        try {
            for (int id : ids) {
                write.write(GenericRow.of(id, BinaryString.fromString(partition)));
            }
            commit.commit(commitIdentifier, write.prepareCommit(true, commitIdentifier));
        } finally {
            write.close();
            commit.close();
        }
    }

    private void writeRows(FileStoreTable table, long commitIdentifier, int... ids)
            throws Exception {
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        try {
            for (int id : ids) {
                write.write(GenericRow.of(id));
            }
            commit.commit(commitIdentifier, write.prepareCommit(true, commitIdentifier));
        } finally {
            write.close();
            commit.close();
        }
    }

    private boolean exists(Path path) {
        try {
            return fileIO.exists(path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
