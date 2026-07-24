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
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestUtils;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    public void testSourceFingerprintIncludesTableLocation() throws Exception {
        Path firstRoot = new Path(tempDir.resolve("location-fingerprint-first/table").toString());
        Path secondRoot = new Path(tempDir.resolve("location-fingerprint-second/table").toString());
        String externalRoot =
                new Path(tempDir.resolve("location-fingerprint-external").toUri()).toString();
        FileStoreTable first = createTable(firstRoot, externalRoot);
        TableSchema identicalSchema = first.schema();
        writeHistoricalSchema(secondRoot, identicalSchema);
        FileStoreTable second = FileStoreTableFactory.create(fileIO, secondRoot, identicalSchema);

        String firstFingerprint = FullHistorySourceFingerprint.compute(first);
        String secondFingerprint = FullHistorySourceFingerprint.compute(second);
        assertThat(firstFingerprint).startsWith("v2:");
        assertThat(firstFingerprint).isNotEqualTo(secondFingerprint);
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
        assertThat(fileIO.readFileUtf8(source.tagManager().tagPath("tag1")))
                .doesNotContain("tagCreateTime", "tagTimeRetained");
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
        assertThat(target.tagManager().tagObjects().get(0).getLeft().getTagCreateTime()).isNull();
        assertThat(target.tagManager().tagObjects().get(0).getLeft().getTagTimeRetained()).isNull();
        assertThat(fileIO.readFileUtf8(target.tagManager().tagPath("tag1")))
                .doesNotContain("tagCreateTime", "tagTimeRetained");
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

        Path missingTarget = targetDataFiles.iterator().next();
        fileIO.delete(missingTarget, false);
        FileStoreTable sourceAtClone = source;
        assertThatThrownBy(
                        () ->
                                new FullHistoryCloneValidator(
                                                sourceAtClone,
                                                target,
                                                mapping,
                                                FullHistoryCopyPlan.empty())
                                        .validatePublishedClone())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Target file does not exist")
                .hasMessageContaining(missingTarget.toString());
    }

    @Test
    public void testStreamingValidationRejectsMissingStatisticsFile() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("statistics-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("statistics-target/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve("statistics-source-external").toUri()).toString();
        String targetExternal =
                new Path(tempDir.resolve("statistics-target-external").toUri()).toString();
        FileStoreTable source = createTable(sourceRoot, sourceExternal);
        writeRows(source, 0, "A", 1);

        Snapshot snapshot = source.snapshotManager().latestSnapshot();
        TableCommitImpl commit = source.newCommit(UUID.randomUUID().toString());
        try {
            commit.updateStatistics(new Statistics(snapshot.id(), snapshot.schemaId(), 1L, 1L));
        } finally {
            commit.close();
        }

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
        Snapshot targetSnapshot = target.snapshotManager().latestSnapshot();
        Path statisticsPath =
                target.store().pathFactory().statsFileFactory().toPath(targetSnapshot.statistics());
        fileIO.delete(statisticsPath, false);

        assertThatThrownBy(
                        () ->
                                new FullHistoryCloneValidator(
                                                source,
                                                target,
                                                mapping,
                                                FullHistoryCopyPlan.empty())
                                        .validatePublishedCloneStreaming())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Target statistics file does not exist")
                .hasMessageContaining(statisticsPath.toString());
    }

    @Test
    public void testRewriteRejectsEscapingStatisticsPath() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("escaping-statistics-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("escaping-statistics-target/table").toString());
        FileStoreTable source = createUnpartitionedTable(sourceRoot, null);
        writeRows(source, 0, 1);
        Snapshot snapshot = source.snapshotManager().latestSnapshot();
        fileIO.overwriteFileUtf8(
                source.snapshotManager().snapshotPath(snapshot.id()),
                copyWithStatistics(snapshot, "../../escaped-statistics").toJson());
        source = FileStoreTableFactory.create(fileIO, sourceRoot);
        PathMapping mapping =
                PathMapping.parse(Collections.singletonList(sourceRoot + "=" + targetRoot));
        FileStoreTable corruptedSource = source;

        assertThatThrownBy(
                        () ->
                                new FullHistoryMetadataRewriter(
                                                corruptedSource, fileIO, targetRoot, mapping)
                                        .rewrite())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Statistics reference must be a file name")
                .hasMessageContaining("../../escaped-statistics");
    }

    @Test
    public void testStreamingValidationRejectsSwappedSnapshotRoots() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("swapped-roots-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("swapped-roots-target/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve("swapped-roots-source-external").toUri()).toString();
        String targetExternal =
                new Path(tempDir.resolve("swapped-roots-target-external").toUri()).toString();
        FileStoreTable source = createTable(sourceRoot, sourceExternal);
        writeRows(source, 0, "A", 1);
        writeRows(source, 1, "B", 2);

        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceExternal + "=" + targetExternal));
        FullHistoryCopyPlan payloadPlan =
                FullHistoryCopyPlan.buildPayload(
                        new FullHistoryFileCollector(source).collect(), mapping, fileIO);
        FullHistoryFileCopier.copy(fileIO, fileIO, payloadPlan, false);
        new FullHistoryMetadataRewriter(source, fileIO, targetRoot, mapping).rewrite();

        FileStoreTable target = FileStoreTableFactory.create(fileIO, targetRoot);
        List<Snapshot> snapshots = target.snapshotManager().safelyGetAllSnapshots();
        assertThat(snapshots).hasSize(2);
        Snapshot first = snapshots.get(0);
        Snapshot second = snapshots.get(1);
        fileIO.overwriteFileUtf8(
                target.snapshotManager().snapshotPath(first.id()),
                copyWithManifestRoots(first, second).toJson());
        fileIO.overwriteFileUtf8(
                target.snapshotManager().snapshotPath(second.id()),
                copyWithManifestRoots(second, first).toJson());
        FileStoreTable corruptedTarget = FileStoreTableFactory.create(fileIO, targetRoot);

        assertThatThrownBy(
                        () ->
                                new FullHistoryCloneValidator(
                                                source,
                                                corruptedTarget,
                                                mapping,
                                                FullHistoryCopyPlan.empty())
                                        .validatePublishedCloneStreaming())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("snapshot")
                .hasMessageContaining("does not match source semantics");
    }

    @Test
    public void testStreamingValidationRejectsCorruptedManifestPruningMetadata() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("corrupted-meta-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("corrupted-meta-target/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve("corrupted-meta-source-external").toUri()).toString();
        String targetExternal =
                new Path(tempDir.resolve("corrupted-meta-target-external").toUri()).toString();
        FileStoreTable source = createTable(sourceRoot, sourceExternal);
        writeRows(source, 0, "A", 1);

        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceExternal + "=" + targetExternal));
        FullHistoryCopyPlan payloadPlan =
                FullHistoryCopyPlan.buildPayload(
                        new FullHistoryFileCollector(source).collect(), mapping, fileIO);
        FullHistoryFileCopier.copy(fileIO, fileIO, payloadPlan, false);
        new FullHistoryMetadataRewriter(source, fileIO, targetRoot, mapping).rewrite();

        FileStoreTable target = FileStoreTableFactory.create(fileIO, targetRoot);
        assertThat(target.newScan().withBucket(0).plan().splits()).isNotEmpty();

        Snapshot snapshot = target.snapshotManager().latestSnapshot();
        ManifestList manifestList = target.store().manifestListFactory().create();
        List<ManifestFileMeta> metas =
                manifestList.read(snapshot.deltaManifestList(), snapshot.deltaManifestListSize());
        assertThat(metas).isNotEmpty();
        List<ManifestFileMeta> corruptedMetas = new ArrayList<>();
        for (ManifestFileMeta meta : metas) {
            corruptedMetas.add(
                    new ManifestFileMeta(
                            meta.fileName(),
                            meta.fileSize(),
                            meta.numAddedFiles(),
                            meta.numDeletedFiles(),
                            meta.partitionStats(),
                            meta.schemaId(),
                            100,
                            100,
                            meta.minLevel(),
                            meta.maxLevel(),
                            meta.minRowId(),
                            meta.maxRowId()));
        }
        Pair<String, Long> corruptedList = manifestList.write(corruptedMetas);
        fileIO.overwriteFileUtf8(
                target.snapshotManager().snapshotPath(snapshot.id()),
                copyWithDeltaManifestList(snapshot, corruptedList).toJson());
        FileStoreTable corruptedTarget = FileStoreTableFactory.create(fileIO, targetRoot);

        assertThat(corruptedTarget.newScan().withBucket(0).plan().splits()).isEmpty();
        assertThatThrownBy(
                        () ->
                                new FullHistoryCloneValidator(
                                                source,
                                                corruptedTarget,
                                                mapping,
                                                FullHistoryCopyPlan.empty())
                                        .validatePublishedCloneStreaming())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("manifest metadata")
                .hasMessageContaining("does not match its entries");
    }

    @Test
    public void testStreamingValidationRejectsCorruptedSharedManifestSize() throws Exception {
        TaggedClone clone = createTaggedClone("corrupted-manifest-size");
        Tag tag = clone.target.tagManager().tagObjects().get(0).getLeft();
        ManifestList manifestList = clone.target.store().manifestListFactory().create();
        List<ManifestFileMeta> metas =
                manifestList.read(tag.deltaManifestList(), tag.deltaManifestListSize());
        assertThat(metas).hasSize(1);
        ManifestFileMeta meta = metas.get(0);
        ManifestFileMeta corruptedMeta =
                new ManifestFileMeta(
                        meta.fileName(),
                        1L,
                        meta.numAddedFiles(),
                        meta.numDeletedFiles(),
                        meta.partitionStats(),
                        meta.schemaId(),
                        meta.minBucket(),
                        meta.maxBucket(),
                        meta.minLevel(),
                        meta.maxLevel(),
                        meta.minRowId(),
                        meta.maxRowId());
        Pair<String, Long> corruptedList =
                manifestList.write(Collections.singletonList(corruptedMeta));
        FileStoreTable corruptedTarget =
                rewriteTagDeltaManifestList(clone.target, tag, corruptedList);

        assertThatThrownBy(
                        () ->
                                corruptedTarget
                                        .copy(
                                                Collections.singletonMap(
                                                        CoreOptions.SCAN_TAG_NAME.key(), "tag1"))
                                        .newScan()
                                        .plan())
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(
                        () ->
                                new FullHistoryCloneValidator(
                                                clone.source,
                                                corruptedTarget,
                                                clone.mapping,
                                                FullHistoryCopyPlan.empty())
                                        .validatePublishedCloneStreaming())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("manifest file")
                .hasMessageContaining("metadata records 1");
    }

    @Test
    public void testStreamingValidationRejectsCorruptedSharedManifestListSize() throws Exception {
        TaggedClone clone = createTaggedClone("corrupted-manifest-list-size");
        Tag tag = clone.target.tagManager().tagObjects().get(0).getLeft();
        assertThat(tag.deltaManifestList())
                .isEqualTo(clone.target.snapshotManager().latestSnapshot().deltaManifestList());
        FileStoreTable corruptedTarget =
                rewriteTagDeltaManifestList(
                        clone.target, tag, Pair.of(tag.deltaManifestList(), 1L));

        assertThatThrownBy(
                        () ->
                                corruptedTarget
                                        .copy(
                                                Collections.singletonMap(
                                                        CoreOptions.SCAN_TAG_NAME.key(), "tag1"))
                                        .newScan()
                                        .plan())
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(
                        () ->
                                new FullHistoryCloneValidator(
                                                clone.source,
                                                corruptedTarget,
                                                clone.mapping,
                                                FullHistoryCopyPlan.empty())
                                        .validatePublishedCloneStreaming())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("manifest list")
                .hasMessageContaining("metadata records 1");
    }

    @Test
    public void testStreamingValidationAllowsManifestRerollAfterPathRewrite() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("reroll-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("reroll-target/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve("reroll-source-external").toUri()).toString();
        StringBuilder targetExternalBuilder =
                new StringBuilder(
                        new Path(tempDir.resolve("reroll-target-external").toUri()).toString());
        for (int i = 0; i < 80; i++) {
            targetExternalBuilder
                    .append("/target-segment-")
                    .append(i)
                    .append("-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
        }
        String targetExternal = targetExternalBuilder.toString();

        Options options = new Options();
        options.set(CoreOptions.PATH, sourceRoot.toString());
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.BUCKET_KEY, "id");
        options.set(CoreOptions.DATA_FILE_EXTERNAL_PATHS, sourceExternal);
        options.set(
                CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY, ExternalPathStrategy.ROUND_ROBIN);
        options.set(CoreOptions.MANIFEST_COMPRESSION, "null");
        options.set("manifest.target-file-size", "4MB");
        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"id"});
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, sourceRoot),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        FileStoreTable source = FileStoreTableFactory.create(fileIO, sourceRoot, schema);
        writeRows(source, 0, 1);

        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 2_000; i++) {
            String fileName = "data-" + i;
            DataFileMeta dataFile =
                    DataFileTestUtils.newFile(fileName, 0, i, i, i)
                            .newExternalPath(new Path(sourceExternal, fileName).toString());
            entries.add(ManifestEntry.create(FileKind.ADD, BinaryRow.EMPTY_ROW, 0, 1, dataFile));
        }
        ManifestFile sourceManifestFile = source.store().manifestFileFactory().create();
        List<ManifestFileMeta> sourceMetas = sourceManifestFile.write(entries);
        assertThat(sourceMetas).hasSize(1);
        Pair<String, Long> sourceManifestList =
                source.store().manifestListFactory().create().write(sourceMetas);
        Snapshot sourceSnapshot = source.snapshotManager().latestSnapshot();
        fileIO.overwriteFileUtf8(
                source.snapshotManager().snapshotPath(sourceSnapshot.id()),
                copyWithDeltaManifestList(sourceSnapshot, sourceManifestList).toJson());
        source = FileStoreTableFactory.create(fileIO, sourceRoot);

        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceExternal + "=" + targetExternal));
        new FullHistoryMetadataRewriter(source, fileIO, targetRoot, mapping).rewrite();
        FileStoreTable target = FileStoreTableFactory.create(fileIO, targetRoot);
        Snapshot targetSnapshot = target.snapshotManager().latestSnapshot();
        List<ManifestFileMeta> targetMetas =
                target.store()
                        .manifestListFactory()
                        .create()
                        .read(
                                targetSnapshot.deltaManifestList(),
                                targetSnapshot.deltaManifestListSize());

        assertThat(targetMetas).hasSizeGreaterThan(sourceMetas.size());
        FullHistoryRootValidator.validate(source, target, mapping, "main");
    }

    @Test
    public void testRetryRejectsTruncatedExtraFile() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("extra-file-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("extra-file-target/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve("extra-file-source-external").toUri()).toString();
        String targetExternal =
                new Path(tempDir.resolve("extra-file-target-external").toUri()).toString();
        Options options = new Options();
        options.set("file-index.bloom-filter.columns", "id");
        options.set("file-index.in-manifest-threshold", "1B");
        FileStoreTable source = createTable(sourceRoot, sourceExternal, options);
        writeRows(source, 0, "A", 1, 2, 3);

        List<Path> sourceExtraFiles = new ArrayList<>();
        new FullHistoryPayloadFileVisitor(source)
                .visit(
                        (path, kind, expectedSize, mappingAnchor) -> {
                            if (expectedSize < 0) {
                                sourceExtraFiles.add(path);
                            }
                        });
        assertThat(sourceExtraFiles).isNotEmpty();

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
        List<Path> targetExtraFiles = new ArrayList<>();
        new FullHistoryPayloadFileVisitor(target)
                .visit(
                        (path, kind, expectedSize, mappingAnchor) -> {
                            if (expectedSize < 0) {
                                targetExtraFiles.add(path);
                            }
                        });
        assertThat(targetExtraFiles).hasSameSizeAs(sourceExtraFiles);
        Path truncated = targetExtraFiles.get(0);
        PayloadStatCountingFileIO validationFileIO = new PayloadStatCountingFileIO(truncated);
        FileStoreTable validationTarget =
                FileStoreTableFactory.create(validationFileIO, targetRoot);
        new FullHistoryCloneValidator(source, validationTarget, mapping)
                .validatePublishedCloneStreaming();
        assertThat(validationFileIO.payloadExistsCalls).isZero();
        assertThat(validationFileIO.payloadSizeCalls).isZero();

        assertThat(fileIO.getFileSize(truncated)).isGreaterThan(1L);
        fileIO.writeFile(truncated, "x", true);

        assertThatThrownBy(() -> FullHistoryFileCopier.copy(fileIO, fileIO, payloadPlan, false))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("different size")
                .hasMessageContaining(truncated.toString());
    }

    @Test
    public void testRewriteTagWithRetention() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("ttl-tag-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("ttl-tag-target/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve("ttl-tag-source-external").toUri()).toString();
        String targetExternal =
                new Path(tempDir.resolve("ttl-tag-target-external").toUri()).toString();
        FileStoreTable source = createTable(sourceRoot, sourceExternal);
        writeRows(source, 0, "A", 1);
        source.createTag("ttl-tag", 1, Duration.ofDays(7));
        Tag sourceTag = source.tagManager().tagObjects().get(0).getLeft();

        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceExternal + "=" + targetExternal));
        FullHistoryCopyPlan payloadPlan =
                FullHistoryCopyPlan.buildPayload(
                        new FullHistoryFileCollector(source).collect(), mapping, fileIO);
        FullHistoryFileCopier.copy(fileIO, fileIO, payloadPlan, false);
        new FullHistoryMetadataRewriter(source, fileIO, targetRoot, mapping).rewrite();

        FileStoreTable target = FileStoreTableFactory.create(fileIO, targetRoot);
        new FullHistoryCloneValidator(source, target, mapping, payloadPlan).validate();
        Tag targetTag = target.tagManager().tagObjects().get(0).getLeft();
        assertThat(targetTag.getTagCreateTime()).isEqualTo(sourceTag.getTagCreateTime());
        assertThat(targetTag.getTagTimeRetained()).isEqualTo(Duration.ofDays(7));
    }

    @Test
    public void testRewriteAbsoluteDataFilePathDirectory() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("path-directory-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("path-directory-target/table").toString());
        Path sourceDataRoot = new Path(tempDir.resolve("path-directory-source-data").toString());
        Path targetDataRoot = new Path(tempDir.resolve("path-directory-target-data").toString());

        Options options = new Options();
        options.set(CoreOptions.PATH, sourceRoot.toString());
        options.set(CoreOptions.BUCKET, -1);
        options.set(CoreOptions.DATA_FILE_PATH_DIRECTORY, sourceDataRoot.toString());
        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"id"});
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, sourceRoot),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        FileStoreTable source = FileStoreTableFactory.create(fileIO, sourceRoot, schema);
        writeRows(source, 0, 1, 2);

        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceDataRoot + "=" + targetDataRoot));
        FullHistoryCopyPlan payloadPlan =
                FullHistoryCopyPlan.buildPayload(
                        new FullHistoryFileCollector(source).collect(), mapping, fileIO);
        FullHistoryFileCopier.copy(fileIO, fileIO, payloadPlan, false);
        new FullHistoryMetadataRewriter(source, fileIO, targetRoot, mapping).rewrite();

        FileStoreTable target = FileStoreTableFactory.create(fileIO, targetRoot);
        new FullHistoryCloneValidator(source, target, mapping, payloadPlan).validate();
        assertThat(target.schema().options())
                .containsEntry(
                        CoreOptions.DATA_FILE_PATH_DIRECTORY.key(), targetDataRoot.toString());
        assertThat(new FullHistoryFileCollector(target).collect().dataFiles())
                .allMatch(path -> path.toString().startsWith(targetDataRoot.toString()))
                .allMatch(this::exists);
    }

    @Test
    public void testRewriteSchemeLessAbsoluteDataFilePathDirectory() {
        Map<String, String> sourceOptions = new HashMap<>();
        sourceOptions.put(CoreOptions.PATH.key(), "hdfs://source/warehouse/table");
        sourceOptions.put(CoreOptions.DATA_FILE_PATH_DIRECTORY.key(), "/cold/data");

        Map<String, String> targetOptions =
                FullHistoryMetadataRewriter.rewriteOptions(
                        sourceOptions,
                        PathMapping.parse(
                                Arrays.asList(
                                        "hdfs://source/warehouse/table="
                                                + "s3://target/warehouse/table",
                                        "hdfs://source/cold/data=s3://target/cold/data")));

        assertThat(targetOptions)
                .containsEntry(CoreOptions.PATH.key(), "s3://target/warehouse/table")
                .containsEntry(CoreOptions.DATA_FILE_PATH_DIRECTORY.key(), "s3://target/cold/data");
    }

    @Test
    public void testRewriteSchemeLessAbsoluteDataDirectoryWithoutPathOption() {
        Map<String, String> sourceOptions = new HashMap<>();
        sourceOptions.put(CoreOptions.DATA_FILE_PATH_DIRECTORY.key(), "/cold/data");

        Map<String, String> targetOptions =
                FullHistoryMetadataRewriter.rewriteOptions(
                        sourceOptions,
                        PathMapping.parse(
                                Arrays.asList(
                                        "hdfs://source/warehouse/table="
                                                + "s3://target/warehouse/table",
                                        "hdfs://source/cold/data=s3://target/cold/data")),
                        new Path("hdfs://source/warehouse/table"));

        assertThat(targetOptions)
                .doesNotContainKey(CoreOptions.PATH.key())
                .containsEntry(CoreOptions.DATA_FILE_PATH_DIRECTORY.key(), "s3://target/cold/data");
    }

    @Test
    public void testRewriteSpecificFsForCrossSchemeMapping() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("specific-fs-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("specific-fs-target/table").toString());
        String sourceExternal1 = "traceable://old-cluster/volume-1/table";
        String sourceExternal2 = "traceable://old-cluster/volume-2/table";
        String targetExternal1 =
                new Path(tempDir.resolve("specific-fs-target-volume-1").toUri()).toString();
        String targetExternal2 =
                new Path(tempDir.resolve("specific-fs-target-volume-2").toUri()).toString();

        Options options = new Options();
        options.set(CoreOptions.PATH, sourceRoot.toString());
        options.set(CoreOptions.BUCKET, -1);
        options.set(CoreOptions.DATA_FILE_EXTERNAL_PATHS, sourceExternal1 + "," + sourceExternal2);
        options.set(
                CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY, ExternalPathStrategy.SPECIFIC_FS);
        options.set(CoreOptions.DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS, "traceable");
        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"id"});
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, sourceRoot),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        FileStoreTable source = FileStoreTableFactory.create(fileIO, sourceRoot, schema);

        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceExternal1 + "=" + targetExternal1,
                                sourceExternal2 + "=" + targetExternal2));
        new FullHistoryMetadataRewriter(source, fileIO, targetRoot, mapping).rewrite();

        FileStoreTable target = FileStoreTableFactory.create(fileIO, targetRoot);
        assertThat(target.schema().options())
                .containsEntry(CoreOptions.DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS.key(), "file")
                .containsEntry(
                        CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(),
                        targetExternal1 + "," + targetExternal2);
        assertThat(target.store().pathFactory().getExternalPaths())
                .extracting(Path::toString)
                .containsExactly(targetExternal1, targetExternal2);
    }

    @Test
    public void testRejectSpecificFsTargetSchemeExpansion() {
        Map<String, String> sourceOptions = new HashMap<>();
        sourceOptions.put(CoreOptions.PATH.key(), "hdfs://source/warehouse/table");
        sourceOptions.put(
                CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(),
                "traceable://source/selected,other://source/unselected");
        sourceOptions.put(
                CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(),
                ExternalPathStrategy.SPECIFIC_FS.toString());
        sourceOptions.put(CoreOptions.DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS.key(), "traceable");

        assertThatThrownBy(
                        () ->
                                FullHistoryMetadataRewriter.rewriteOptions(
                                        sourceOptions,
                                        PathMapping.parse(
                                                Arrays.asList(
                                                        "hdfs://source/warehouse/table="
                                                                + "s3://target/warehouse/table",
                                                        "traceable://source/selected="
                                                                + "file:/target/selected",
                                                        "other://source/unselected="
                                                                + "file:/target/unselected"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("would select additional external paths")
                .hasMessageContaining("index 1");
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
                                        .planStructure())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(CoreOptions.BLOB_DESCRIPTOR_FIELD.key())
                .hasMessageContaining("inside data files");
    }

    @Test
    public void testPlannerRejectsIcebergCompatibilityInHistoricalSchema() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("iceberg-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("iceberg-target/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve("iceberg-source-external").toUri()).toString();
        String targetExternal =
                new Path(tempDir.resolve("iceberg-target-external").toUri()).toString();
        FileStoreTable source = createTable(sourceRoot, sourceExternal);
        source.schemaManager()
                .commitChanges(
                        SchemaChange.setOption(
                                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                                IcebergOptions.StorageType.TABLE_LOCATION.toString()));
        source.schemaManager()
                .commitChanges(
                        SchemaChange.setOption(
                                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                                IcebergOptions.StorageType.DISABLED.toString()));
        source = FileStoreTableFactory.create(fileIO, sourceRoot);
        assertThat(
                        Options.fromMap(source.schema().options())
                                .get(IcebergOptions.METADATA_ICEBERG_STORAGE))
                .isEqualTo(IcebergOptions.StorageType.DISABLED);
        FileStoreTable finalSource = source;

        assertThatThrownBy(
                        () ->
                                new FullHistoryClonePlanner(
                                                finalSource,
                                                PathMapping.parse(
                                                        Arrays.asList(
                                                                sourceRoot + "=" + targetRoot,
                                                                sourceExternal
                                                                        + "="
                                                                        + targetExternal)))
                                        .planStructure())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(IcebergOptions.METADATA_ICEBERG_STORAGE.key())
                .hasMessageContaining("table-location")
                .hasMessageContaining("not copied");
        assertThat(fileIO.exists(targetRoot)).isFalse();
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
    public void testPlannerRejectsMissingAbsoluteDataPathDirectoryMapping() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("missing-data-path-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("missing-data-path-target/table").toString());
        Path sourceDataRoot = new Path(tempDir.resolve("missing-data-path-source-data").toString());
        Options options = new Options();
        options.set(CoreOptions.PATH, sourceRoot.toString());
        options.set(CoreOptions.BUCKET, -1);
        options.set(CoreOptions.DATA_FILE_PATH_DIRECTORY, sourceDataRoot.toString());
        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"id"});
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, sourceRoot),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        FileStoreTable source = FileStoreTableFactory.create(fileIO, sourceRoot, schema);

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
                .hasMessageContaining(sourceDataRoot.toString());
    }

    @Test
    public void testPlannerRejectsSpecificFsMappedToMultipleSchemes() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("mixed-specific-fs-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("mixed-specific-fs-target/table").toString());
        String sourceExternal1 = "traceable://old-cluster/volume-1/table";
        String sourceExternal2 = "traceable://old-cluster/volume-2/table";
        Options options = new Options();
        options.set(CoreOptions.PATH, sourceRoot.toString());
        options.set(CoreOptions.BUCKET, -1);
        options.set(CoreOptions.DATA_FILE_EXTERNAL_PATHS, sourceExternal1 + "," + sourceExternal2);
        options.set(
                CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY, ExternalPathStrategy.SPECIFIC_FS);
        options.set(CoreOptions.DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS, "traceable");
        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"id"});
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, sourceRoot),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        FileStoreTable source = FileStoreTableFactory.create(fileIO, sourceRoot, schema);

        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceExternal1 + "=s3://new-cluster/volume-1/table",
                                sourceExternal2 + "=oss://new-cluster/volume-2/table"));
        assertThatThrownBy(() -> new FullHistoryClonePlanner(source, mapping).planStructure())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one target file system")
                .hasMessageContaining("oss")
                .hasMessageContaining("s3");
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
    public void testInternalIndexFollowsTableRootMapping() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("internal-index-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve("internal-index-target/table").toString());
        Path nestedTargetRoot =
                new Path(tempDir.resolve("internal-index-unexpected-target").toString());
        FileStoreTable source = createUnpartitionedTable(sourceRoot, null);
        writeRows(source, 0, 1);

        String indexFileName = "index-test";
        Path sourceIndexRoot = new Path(sourceRoot, "index");
        Path sourceIndexPath = new Path(sourceIndexRoot, indexFileName);
        fileIO.writeFile(sourceIndexPath, "index-content", false);
        IndexManifestEntry sourceIndexEntry =
                new IndexManifestEntry(
                        FileKind.ADD,
                        BinaryRow.EMPTY_ROW,
                        0,
                        new IndexFileMeta(
                                "test-index",
                                indexFileName,
                                fileIO.getFileSize(sourceIndexPath),
                                1,
                                null,
                                null,
                                null));
        String indexManifest =
                source.store()
                        .indexManifestFileFactory()
                        .create()
                        .writeWithoutRolling(Collections.singletonList(sourceIndexEntry));
        Snapshot snapshot = source.snapshotManager().latestSnapshot();
        fileIO.overwriteFileUtf8(
                source.snapshotManager().snapshotPath(snapshot.id()),
                copyWithIndexManifest(snapshot, indexManifest).toJson());
        source = FileStoreTableFactory.create(fileIO, sourceRoot);

        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceIndexRoot + "=" + nestedTargetRoot));
        FullHistoryCopyPlan payloadPlan =
                FullHistoryCopyPlan.buildPayload(
                        new FullHistoryFileCollector(source).collect(), mapping, fileIO);
        assertThat(payloadPlan.files())
                .filteredOn(file -> file.kind() == FullHistoryCopyPlan.FileKind.INDEX)
                .extracting(file -> file.target().toString())
                .containsExactly(new Path(targetRoot, "index/" + indexFileName).toString());

        FullHistoryFileCopier.copy(fileIO, fileIO, payloadPlan, false);
        new FullHistoryMetadataRewriter(source, fileIO, targetRoot, mapping).rewrite();
        FileStoreTable target = FileStoreTableFactory.create(fileIO, targetRoot);
        new FullHistoryCloneValidator(source, target, mapping, payloadPlan).validate();
        assertThat(fileIO.exists(new Path(nestedTargetRoot, indexFileName))).isFalse();
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

    private TaggedClone createTaggedClone(String prefix) throws Exception {
        Path sourceRoot = new Path(tempDir.resolve(prefix + "-source/table").toString());
        Path targetRoot = new Path(tempDir.resolve(prefix + "-target/table").toString());
        String sourceExternal =
                new Path(tempDir.resolve(prefix + "-source-external").toUri()).toString();
        String targetExternal =
                new Path(tempDir.resolve(prefix + "-target-external").toUri()).toString();
        FileStoreTable source = createTable(sourceRoot, sourceExternal);
        writeRows(source, 0, "A", 1);
        source.createTag("tag1", 1);

        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceExternal + "=" + targetExternal));
        FullHistoryCopyPlan payloadPlan =
                FullHistoryCopyPlan.buildPayload(
                        new FullHistoryFileCollector(source).collect(), mapping, fileIO);
        FullHistoryFileCopier.copy(fileIO, fileIO, payloadPlan, false);
        new FullHistoryMetadataRewriter(source, fileIO, targetRoot, mapping).rewrite();
        return new TaggedClone(source, FileStoreTableFactory.create(fileIO, targetRoot), mapping);
    }

    private FileStoreTable rewriteTagDeltaManifestList(
            FileStoreTable target, Tag tag, Pair<String, Long> deltaManifestList) throws Exception {
        Snapshot corruptedSnapshot = copyWithDeltaManifestList(tag, deltaManifestList);
        Tag corruptedTag =
                Tag.fromSnapshotAndTagTtl(
                        corruptedSnapshot, tag.getTagTimeRetained(), tag.getTagCreateTime());
        fileIO.overwriteFileUtf8(target.tagManager().tagPath("tag1"), corruptedTag.toJson());
        return FileStoreTableFactory.create(fileIO, target.location());
    }

    private static class TaggedClone {

        private final FileStoreTable source;
        private final FileStoreTable target;
        private final PathMapping mapping;

        private TaggedClone(FileStoreTable source, FileStoreTable target, PathMapping mapping) {
            this.source = source;
            this.target = target;
            this.mapping = mapping;
        }
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
        if (indexExternalRoot != null) {
            options.set(CoreOptions.GLOBAL_INDEX_EXTERNAL_PATH, indexExternalRoot);
        }
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

    private static Snapshot copyWithStatistics(Snapshot snapshot, String statistics) {
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
                snapshot.indexManifest(),
                snapshot.commitUser(),
                snapshot.commitIdentifier(),
                snapshot.commitKind(),
                snapshot.timeMillis(),
                snapshot.totalRecordCount(),
                snapshot.deltaRecordCount(),
                snapshot.changelogRecordCount(),
                snapshot.watermark(),
                statistics,
                snapshot.properties(),
                snapshot.nextRowId(),
                snapshot.operation());
    }

    private static Snapshot copyWithManifestRoots(Snapshot snapshot, Snapshot roots) {
        return new Snapshot(
                snapshot.version(),
                snapshot.id(),
                snapshot.schemaId(),
                roots.baseManifestList(),
                roots.baseManifestListSize(),
                roots.deltaManifestList(),
                roots.deltaManifestListSize(),
                roots.changelogManifestList(),
                roots.changelogManifestListSize(),
                roots.indexManifest(),
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

    private static Snapshot copyWithDeltaManifestList(
            Snapshot snapshot, Pair<String, Long> deltaManifestList) {
        return new Snapshot(
                snapshot.version(),
                snapshot.id(),
                snapshot.schemaId(),
                snapshot.baseManifestList(),
                snapshot.baseManifestListSize(),
                deltaManifestList.getLeft(),
                deltaManifestList.getRight(),
                snapshot.changelogManifestList(),
                snapshot.changelogManifestListSize(),
                snapshot.indexManifest(),
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

    private void writeHistoricalSchema(Path tableRoot, TableSchema schema) throws Exception {
        SchemaManager manager = new SchemaManager(fileIO, tableRoot);
        Path schemaPath =
                new Path(manager.schemaDirectory(), SchemaManager.SCHEMA_PREFIX + schema.id());
        assertThat(fileIO.tryToWriteAtomic(schemaPath, schema.toString())).isTrue();
    }

    private static class PayloadStatCountingFileIO extends LocalFileIO {

        private final Path payload;
        private int payloadExistsCalls;
        private int payloadSizeCalls;

        private PayloadStatCountingFileIO(Path payload) {
            this.payload = payload;
        }

        @Override
        public boolean exists(Path path) throws java.io.IOException {
            if (payload.equals(path)) {
                payloadExistsCalls++;
            }
            return super.exists(path);
        }

        @Override
        public long getFileSize(Path path) throws java.io.IOException {
            if (payload.equals(path)) {
                payloadSizeCalls++;
            }
            return super.getFileSize(path);
        }
    }
}
