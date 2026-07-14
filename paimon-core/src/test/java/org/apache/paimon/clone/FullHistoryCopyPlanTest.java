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

import org.apache.paimon.fs.Path;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FullHistoryCopyPlan}. */
public class FullHistoryCopyPlanTest {

    @Test
    public void testBuildCopyPlanWithPathMapping() {
        FullHistoryFileSet.Builder builder = FullHistoryFileSet.builder();
        builder.addMetadataFile(new Path("dfs://old/warehouse/db/t/schema/schema-0"));
        builder.addDataFile(new Path("dfs://old/external/db/t/pt=A/bucket-0/data.orc"));
        builder.addIndexFile(new Path("dfs://old/warehouse/db/t/index/index-1"));
        FullHistoryFileSet files = builder.build();
        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                "dfs://old/warehouse=dfs://new/warehouse",
                                "dfs://old/external=dfs://new/external"));

        FullHistoryCopyPlan plan = FullHistoryCopyPlan.build(files, mapping);

        assertThat(plan.files()).hasSize(3);
        assertThat(plan.files())
                .extracting(file -> file.source().toString())
                .containsExactly(
                        "dfs://old/warehouse/db/t/schema/schema-0",
                        "dfs://old/external/db/t/pt=A/bucket-0/data.orc",
                        "dfs://old/warehouse/db/t/index/index-1");
        assertThat(plan.files())
                .extracting(file -> file.target().toString())
                .containsExactly(
                        "dfs://new/warehouse/db/t/schema/schema-0",
                        "dfs://new/external/db/t/pt=A/bucket-0/data.orc",
                        "dfs://new/warehouse/db/t/index/index-1");
        assertThat(plan.files())
                .extracting(FullHistoryCopyPlan.FileCopy::kind)
                .containsExactly(
                        FullHistoryCopyPlan.FileKind.METADATA,
                        FullHistoryCopyPlan.FileKind.DATA,
                        FullHistoryCopyPlan.FileKind.INDEX);
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
        builder.addMetadataFile(new Path("dfs://old/warehouse/db/t/schema/schema-0"));
        builder.addDataFile(new Path("dfs://unknown/db/t/pt=A/bucket-0/data.orc"));
        FullHistoryFileSet files = builder.build();
        PathMapping mapping =
                PathMapping.parse(
                        Collections.singletonList("dfs://old/warehouse=dfs://new/warehouse"));

        assertThatThrownBy(() -> FullHistoryCopyPlan.build(files, mapping))
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
    public void testFilesAreImmutable() {
        FullHistoryFileSet.Builder builder = FullHistoryFileSet.builder();
        builder.addMetadataFile(new Path("dfs://old/warehouse/db/t/schema/schema-0"));
        FullHistoryFileSet files = builder.build();
        FullHistoryCopyPlan plan =
                FullHistoryCopyPlan.build(
                        files,
                        PathMapping.parse(
                                Collections.singletonList(
                                        "dfs://old/warehouse=dfs://new/warehouse")));

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
