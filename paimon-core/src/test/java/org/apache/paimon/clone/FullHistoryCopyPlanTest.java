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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for {@link FullHistoryCopyPlan}. */
public class FullHistoryCopyPlanTest {

    @Test
    public void testBuildPayloadCopyPlanWithPathMapping() throws Exception {
        FullHistoryFileSet.Builder builder = FullHistoryFileSet.builder();
        builder.addDataFile(new Path("dfs://old/external/db/t/pt=A/bucket-0/data.orc"));
        builder.addIndexFile(new Path("dfs://old/warehouse/db/t/index/index-1"));
        FullHistoryFileSet files = builder.build();
        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                "dfs://old/warehouse=dfs://new/warehouse",
                                "dfs://old/external=dfs://new/external"));

        FileIO fileIO = mock(FileIO.class);
        when(fileIO.getFileSize(any(Path.class))).thenReturn(1L);
        FullHistoryCopyPlan plan = FullHistoryCopyPlan.buildPayload(files, mapping, fileIO);

        assertThat(plan.files()).hasSize(2);
        assertThat(plan.files())
                .extracting(file -> file.source().toString())
                .containsExactly(
                        "dfs://old/external/db/t/pt=A/bucket-0/data.orc",
                        "dfs://old/warehouse/db/t/index/index-1");
        assertThat(plan.files())
                .extracting(file -> file.target().toString())
                .containsExactly(
                        "dfs://new/external/db/t/pt=A/bucket-0/data.orc",
                        "dfs://new/warehouse/db/t/index/index-1");
        assertThat(plan.files())
                .extracting(FullHistoryCopyPlan.FileCopy::kind)
                .containsExactly(
                        FullHistoryCopyPlan.FileKind.DATA, FullHistoryCopyPlan.FileKind.INDEX);
    }

    @Test
    public void testInternalPayloadUsesMappingAnchor() throws Exception {
        Path tableRoot = new Path("dfs://old/warehouse/db/t");
        FullHistoryFileSet.Builder builder = FullHistoryFileSet.builder();
        builder.addDataFile(
                new Path("dfs://old/warehouse/db/t/data/bucket-0/internal.orc"), tableRoot);
        builder.addDataFile(new Path("dfs://old/warehouse/db/t/data/bucket-0/external.orc"));

        FileIO fileIO = mock(FileIO.class);
        when(fileIO.getFileSize(any(Path.class))).thenReturn(1L);
        FullHistoryCopyPlan plan =
                FullHistoryCopyPlan.buildPayload(
                        builder.build(),
                        PathMapping.parse(
                                Arrays.asList(
                                        "dfs://old/warehouse/db/t=dfs://new/warehouse/db/t",
                                        "dfs://old/warehouse/db/t/data=dfs://new/external")),
                        fileIO);

        assertThat(plan.files())
                .extracting(file -> file.target().toString())
                .containsExactly(
                        "dfs://new/warehouse/db/t/data/bucket-0/internal.orc",
                        "dfs://new/external/bucket-0/external.orc");
    }

    @Test
    public void testPayloadPlanExcludesMetadataAndRecordsSize() throws Exception {
        org.apache.paimon.fs.FileIO fileIO = org.apache.paimon.fs.local.LocalFileIO.create();
        java.nio.file.Path sourceRoot = java.nio.file.Files.createTempDirectory("copy-plan-source");
        java.nio.file.Path targetRoot = java.nio.file.Files.createTempDirectory("copy-plan-target");
        Path metadata = new Path(sourceRoot.resolve("schema/schema-0").toString());
        Path data = new Path(sourceRoot.resolve("data/data.orc").toString());
        Path index = new Path(sourceRoot.resolve("index/index-0").toString());
        fileIO.writeFile(metadata, "schema", false);
        fileIO.writeFile(data, "data-file", false);
        fileIO.writeFile(index, "index", false);

        FullHistoryFileSet.Builder builder = FullHistoryFileSet.builder();
        builder.addMetadataFile(metadata);
        builder.addDataFile(data);
        builder.addIndexFile(index);
        FullHistoryCopyPlan plan =
                FullHistoryCopyPlan.buildPayload(
                        builder.build(),
                        PathMapping.parse(Collections.singletonList(sourceRoot + "=" + targetRoot)),
                        fileIO);

        assertThat(plan.files()).hasSize(2);
        assertThat(plan.files())
                .extracting(FullHistoryCopyPlan.FileCopy::kind)
                .containsExactly(
                        FullHistoryCopyPlan.FileKind.DATA, FullHistoryCopyPlan.FileKind.INDEX);
        assertThat(plan.files())
                .extracting(FullHistoryCopyPlan.FileCopy::expectedSize)
                .containsExactly(9L, 5L);
    }

    @Test
    public void testUnmatchedPathFails() {
        FullHistoryFileSet.Builder builder = FullHistoryFileSet.builder();
        builder.addDataFile(new Path("dfs://unknown/db/t/pt=A/bucket-0/data.orc"));
        FullHistoryFileSet files = builder.build();
        PathMapping mapping =
                PathMapping.parse(
                        Collections.singletonList("dfs://old/warehouse=dfs://new/warehouse"));

        assertThatThrownBy(
                        () -> FullHistoryCopyPlan.buildPayload(files, mapping, mock(FileIO.class)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No path mapping");
    }

    @Test
    public void testTargetConflictFailsDuringMapping() {
        assertThatThrownBy(
                        () ->
                                PathMapping.parse(
                                        Arrays.asList(
                                                "dfs://old/a=dfs://new/same",
                                                "dfs://old/b=dfs://new/same")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Target path mapping prefixes must not overlap");
    }

    @Test
    public void testCaseOnlyLocalTargetFileConflictFails() {
        FullHistoryFileSet.Builder builder = FullHistoryFileSet.builder();
        builder.addDataFile(new Path("s3://source/table/data/File.orc"));
        builder.addDataFile(new Path("s3://source/table/data/file.orc"));

        assertThatThrownBy(
                        () ->
                                FullHistoryCopyPlan.buildPayload(
                                        builder.build(),
                                        PathMapping.parse(
                                                Collections.singletonList(
                                                        "s3://source/table=/tmp/target")),
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Found target path conflict");
    }

    @Test
    public void testFilesAreImmutable() {
        FullHistoryCopyPlan plan = FullHistoryCopyPlan.empty();

        List<FullHistoryCopyPlan.FileCopy> fileCopies = plan.files();
        assertThatThrownBy(
                        () ->
                                fileCopies.add(
                                        new FullHistoryCopyPlan.FileCopy(
                                                new Path("dfs://old/another"),
                                                new Path("dfs://new/another"))))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
