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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ExternalPathStrategy;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FullHistoryCloneMarker}. */
public class FullHistoryCloneMarkerTest {

    @TempDir private java.nio.file.Path tempDir;

    private final FileIO fileIO = LocalFileIO.create();

    @Test
    public void testPrepareAndResumeMatchingClone() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("source").toString());
        Path targetRoot = new Path(tempDir.resolve("target").toString());
        PathMapping mapping = mapping(sourceRoot, targetRoot);
        FileStoreTable source = createTable(sourceRoot);

        assertThat(
                        FullHistoryCloneMarker.prepare(
                                fileIO,
                                new FullHistoryClonePlanner(source, mapping).planStructure(),
                                mapping,
                                false))
                .isFalse();

        assertThat(fileIO.exists(new Path(targetRoot, FullHistoryCloneMarker.FILE_NAME))).isTrue();
        assertThat(
                        FullHistoryCloneMarker.prepare(
                                fileIO,
                                new FullHistoryClonePlanner(source, mapping).planStructure(),
                                mapping,
                                true))
                .isTrue();
    }

    @Test
    public void testPrepareRejectsPopulatedExternalTargetRoot() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("source-external-table").toString());
        Path targetRoot = new Path(tempDir.resolve("target-external-table").toString());
        Path sourceExternal = new Path(tempDir.resolve("source-external-data").toString());
        Path targetExternal = new Path(tempDir.resolve("target-external-data").toString());
        FileStoreTable source = createTable(sourceRoot, sourceExternal);
        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceExternal + "=" + targetExternal));
        FullHistoryClonePlan plan = new FullHistoryClonePlanner(source, mapping).planStructure();
        fileIO.writeFile(new Path(targetExternal, "unrelated"), "data", false);

        assertThat(plan.externalTargetRoots()).containsExactly(targetExternal);
        assertThatThrownBy(() -> FullHistoryCloneMarker.prepare(fileIO, plan, mapping, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("external target root")
                .hasMessageContaining(targetExternal.toString());
        assertThat(fileIO.exists(new Path(targetRoot, FullHistoryCloneMarker.FILE_NAME))).isFalse();
    }

    @Test
    public void testResumeRequiresMatchingExternalTargetMarker() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("resume-source-table").toString());
        Path targetRoot = new Path(tempDir.resolve("resume-target-table").toString());
        Path sourceExternal = new Path(tempDir.resolve("resume-source-external").toString());
        Path targetExternal = new Path(tempDir.resolve("resume-target-external").toString());
        FileStoreTable source = createTable(sourceRoot, sourceExternal);
        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceExternal + "=" + targetExternal));
        FullHistoryClonePlan plan = new FullHistoryClonePlanner(source, mapping).planStructure();

        assertThat(FullHistoryCloneMarker.prepare(fileIO, plan, mapping, false)).isFalse();
        Path externalMarker = new Path(targetExternal, FullHistoryCloneMarker.FILE_NAME);
        assertThat(fileIO.exists(externalMarker)).isTrue();
        fileIO.writeFile(new Path(targetExternal, "payload"), "data", false);
        assertThat(FullHistoryCloneMarker.prepare(fileIO, plan, mapping, true)).isTrue();

        fileIO.delete(externalMarker, false);
        assertThatThrownBy(() -> FullHistoryCloneMarker.prepare(fileIO, plan, mapping, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("external target root")
                .hasMessageContaining("not owned");
    }

    @Test
    public void testPlannerCoalescesNestedExternalTargetRoots() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("nested-source-table").toString());
        Path targetRoot = new Path(tempDir.resolve("nested-target-table").toString());
        Path sourceExternal = new Path(tempDir.resolve("nested-source-external").toUri());
        Path targetExternal = new Path(tempDir.resolve("nested-target-external").toUri());
        FileStoreTable source =
                createTable(sourceRoot, sourceExternal, new Path(sourceExternal, "index"));
        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceExternal + "=" + targetExternal));

        FullHistoryClonePlan plan = new FullHistoryClonePlanner(source, mapping).planStructure();

        assertThat(plan.externalTargetRoots()).containsExactly(targetExternal);
    }

    @Test
    public void testPlannerOwnsNestedExternalMappingTargets() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("mapped-nested-source-table").toString());
        Path targetRoot = new Path(tempDir.resolve("mapped-nested-target-table").toString());
        Path sourceExternal = new Path(tempDir.resolve("mapped-nested-source-external").toUri());
        Path targetExternal = new Path(tempDir.resolve("mapped-nested-target-external").toUri());
        Path nestedSource = new Path(sourceExternal, "bucket-0");
        Path nestedTarget = new Path(tempDir.resolve("mapped-nested-target-bucket").toUri());
        FileStoreTable source = createTable(sourceRoot, sourceExternal);
        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceExternal + "=" + targetExternal,
                                nestedSource + "=" + nestedTarget));

        FullHistoryClonePlan plan = new FullHistoryClonePlanner(source, mapping).planStructure();

        assertThat(plan.externalTargetRoots())
                .containsExactlyInAnyOrder(targetExternal, nestedTarget);
        FullHistoryCloneMarker.prepare(fileIO, plan, mapping, false);
        assertThat(fileIO.exists(new Path(targetExternal, FullHistoryCloneMarker.FILE_NAME)))
                .isTrue();
        assertThat(fileIO.exists(new Path(nestedTarget, FullHistoryCloneMarker.FILE_NAME)))
                .isTrue();
    }

    @Test
    public void testPlanRejectsPayloadOutsideOwnedRoots() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("guard-source-table").toString());
        Path targetRoot = new Path(tempDir.resolve("guard-target-table").toString());
        Path sourceExternal = new Path(tempDir.resolve("guard-source-external").toUri());
        Path targetExternal = new Path(tempDir.resolve("guard-target-external").toUri());
        FileStoreTable source = createTable(sourceRoot, sourceExternal);
        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceExternal + "=" + targetExternal));
        FullHistoryClonePlan plan = new FullHistoryClonePlanner(source, mapping).planStructure();

        assertThatThrownBy(
                        () ->
                                plan.validatePayloadTarget(
                                        new Path(tempDir.resolve("unowned/file").toUri())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside full-history clone owned roots");
    }

    @Test
    public void testPlanRejectsPayloadInControlNamespace() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("reserved-source-table").toString());
        Path targetRoot = new Path(tempDir.resolve("reserved-target-table").toString());
        Path sourceExternal = new Path(tempDir.resolve("reserved-source-external").toUri());
        Path targetExternal = new Path(tempDir.resolve("reserved-target-external").toUri());
        FileStoreTable source = createTable(sourceRoot, sourceExternal);
        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                sourceExternal + "=" + targetExternal));
        FullHistoryClonePlan plan = new FullHistoryClonePlanner(source, mapping).planStructure();

        assertThatThrownBy(
                        () ->
                                plan.validatePayloadTarget(
                                        new Path(
                                                targetRoot,
                                                FullHistoryCloneMarker.SUCCESS_FILE_NAME)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved control namespace");
        assertThatThrownBy(
                        () ->
                                plan.validatePayloadTarget(
                                        new Path(
                                                targetRoot,
                                                FullHistoryCloneMarker.SUCCESS_FILE_NAME
                                                        .toLowerCase())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved control namespace");
        assertThatThrownBy(
                        () ->
                                plan.validatePayloadTarget(
                                        new Path(
                                                new Path(
                                                        targetExternal,
                                                        FullHistoryCloneMarker.FILE_NAME),
                                                "forged-payload")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved control namespace");
    }

    @Test
    public void testRejectUnownedOrDifferentClone() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("source-reject").toString());
        Path targetRoot = new Path(tempDir.resolve("target-reject").toString());
        FileStoreTable source = createTable(sourceRoot);
        PathMapping mapping = mapping(sourceRoot, targetRoot);
        fileIO.writeFile(new Path(targetRoot, "unrelated"), "data", false);

        assertThatThrownBy(
                        () ->
                                FullHistoryCloneMarker.prepare(
                                        fileIO,
                                        new FullHistoryClonePlanner(source, mapping)
                                                .planStructure(),
                                        mapping,
                                        true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not owned");

        fileIO.delete(targetRoot, true);
        FullHistoryCloneMarker.prepare(
                fileIO,
                new FullHistoryClonePlanner(source, mapping).planStructure(),
                mapping,
                false);
        PathMapping differentMapping =
                PathMapping.parse(
                        Arrays.asList(
                                sourceRoot + "=" + targetRoot,
                                tempDir.resolve("unused-source")
                                        + "="
                                        + tempDir.resolve("unused-target")));
        assertThatThrownBy(
                        () ->
                                FullHistoryCloneMarker.prepare(
                                        fileIO,
                                        new FullHistoryClonePlanner(source, differentMapping)
                                                .planStructure(),
                                        differentMapping,
                                        true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testRejectResumeAfterSourceChanges() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("source-changed").toString());
        Path targetRoot = new Path(tempDir.resolve("target-changed").toString());
        PathMapping mapping = mapping(sourceRoot, targetRoot);
        FileStoreTable source = createTable(sourceRoot);
        FullHistoryCloneMarker.prepare(
                fileIO,
                new FullHistoryClonePlanner(source, mapping).planStructure(),
                mapping,
                false);

        source.schemaManager().commitChanges(SchemaChange.setOption("changed", "true"));
        FileStoreTable changedSource = FileStoreTableFactory.create(fileIO, sourceRoot);

        assertThatThrownBy(
                        () ->
                                FullHistoryCloneMarker.prepare(
                                        fileIO,
                                        new FullHistoryClonePlanner(changedSource, mapping)
                                                .planStructure(),
                                        mapping,
                                        true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("different full-history clone");
    }

    @Test
    public void testMarkSuccessfulClone() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("source-success").toString());
        Path targetRoot = new Path(tempDir.resolve("target-success").toString());
        PathMapping mapping = mapping(sourceRoot, targetRoot);
        FileStoreTable source = createTable(sourceRoot);
        FullHistoryClonePlan plan = new FullHistoryClonePlanner(source, mapping).planStructure();
        FullHistoryCloneMarker.prepare(fileIO, plan, mapping, false);

        assertThat(fileIO.exists(new Path(targetRoot, FullHistoryCloneMarker.SUCCESS_FILE_NAME)))
                .isFalse();
        assertThat(FullHistoryCloneMarker.isSuccessful(fileIO, plan, mapping, null, null))
                .isFalse();
        FullHistoryCloneMarker.markSuccessful(fileIO, plan, mapping, null, null);
        assertThat(fileIO.exists(new Path(targetRoot, FullHistoryCloneMarker.SUCCESS_FILE_NAME)))
                .isTrue();
        assertThat(FullHistoryCloneMarker.isSuccessful(fileIO, plan, mapping, null, null)).isTrue();

        assertThatThrownBy(
                        () ->
                                FullHistoryCloneMarker.prepare(
                                        fileIO,
                                        new FullHistoryClonePlanner(source, mapping)
                                                .planStructure(),
                                        mapping,
                                        true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already completed");
    }

    @Test
    public void testRejectSuccessMarkerFromDifferentClone() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("source-corrupt-success").toString());
        Path targetRoot = new Path(tempDir.resolve("target-corrupt-success").toString());
        PathMapping mapping = mapping(sourceRoot, targetRoot);
        FileStoreTable source = createTable(sourceRoot);
        FullHistoryCloneMarker.prepare(
                fileIO,
                new FullHistoryClonePlanner(source, mapping).planStructure(),
                mapping,
                false);
        fileIO.writeFile(
                new Path(targetRoot, FullHistoryCloneMarker.SUCCESS_FILE_NAME),
                "different clone",
                false);

        assertThatThrownBy(
                        () ->
                                FullHistoryCloneMarker.prepare(
                                        fileIO,
                                        new FullHistoryClonePlanner(source, mapping)
                                                .planStructure(),
                                        mapping,
                                        true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Success marker")
                .hasMessageContaining("different clone");
    }

    @Test
    public void testRejectChangedOwnershipMarkerWhenMarkingSuccessful() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("source-changed-marker").toString());
        Path targetRoot = new Path(tempDir.resolve("target-changed-marker").toString());
        PathMapping mapping = mapping(sourceRoot, targetRoot);
        FileStoreTable source = createTable(sourceRoot);
        FullHistoryClonePlan plan = new FullHistoryClonePlanner(source, mapping).planStructure();
        FullHistoryCloneMarker.prepare(fileIO, plan, mapping, false);
        fileIO.overwriteFileUtf8(
                new Path(targetRoot, FullHistoryCloneMarker.FILE_NAME), "different clone");

        assertThatThrownBy(
                        () ->
                                FullHistoryCloneMarker.markSuccessful(
                                        fileIO, plan, mapping, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("belongs to a different full-history clone");
    }

    @Test
    public void testTargetIdentifierIsPartOfCloneIdentity() throws Exception {
        Path sourceRoot = new Path(tempDir.resolve("source-identifier").toString());
        Path targetRoot = new Path(tempDir.resolve("target-identifier").toString());
        PathMapping mapping = mapping(sourceRoot, targetRoot);
        FileStoreTable source = createTable(sourceRoot);
        FullHistoryCloneMarker.prepare(
                fileIO,
                new FullHistoryClonePlanner(source, mapping).planStructure(),
                mapping,
                "target_db",
                "target_table",
                false);

        assertThatThrownBy(
                        () ->
                                FullHistoryCloneMarker.prepare(
                                        fileIO,
                                        new FullHistoryClonePlanner(source, mapping)
                                                .planStructure(),
                                        mapping,
                                        "other_db",
                                        "target_table",
                                        true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("different full-history clone");
    }

    private FileStoreTable createTable(Path path) throws Exception {
        return createTable(path, null);
    }

    private FileStoreTable createTable(Path path, Path externalPath) throws Exception {
        return createTable(path, externalPath, null);
    }

    private FileStoreTable createTable(Path path, Path externalPath, Path globalIndexExternalPath)
            throws Exception {
        Schema.Builder builder =
                Schema.newBuilder().column("id", DataTypes.INT()).option("path", path.toString());
        if (externalPath != null) {
            builder.option(CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(), externalPath.toString());
            builder.option(
                    CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(),
                    ExternalPathStrategy.ROUND_ROBIN.toString());
        }
        if (globalIndexExternalPath != null) {
            builder.option(
                    CoreOptions.GLOBAL_INDEX_EXTERNAL_PATH.key(),
                    globalIndexExternalPath.toString());
        }
        Schema schema = builder.build();
        SchemaUtils.forceCommit(new SchemaManager(fileIO, path), schema);
        return FileStoreTableFactory.create(fileIO, path);
    }

    private static PathMapping mapping(Path source, Path target) {
        return PathMapping.parse(Collections.singletonList(source + "=" + target));
    }
}
