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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PathMapping}. */
public class PathMappingTest {

    @Test
    public void testLongestPrefixWins() {
        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                "dfs://old/warehouse=dfs://new/warehouse",
                                "dfs://old/warehouse/external=dfs://new/external"));

        assertThat(mapping.rewriteRequired("dfs://old/warehouse/external/db/t/file.orc"))
                .isEqualTo("dfs://new/external/db/t/file.orc");
        assertThat(mapping.rewriteRequired("dfs://old/warehouse/db/t/file.orc"))
                .isEqualTo("dfs://new/warehouse/db/t/file.orc");
    }

    @Test
    public void testRewriteUnderAnchorIgnoresNestedMapping() {
        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                "dfs://old/warehouse/table=dfs://new/warehouse/table",
                                "dfs://old/warehouse/table/data=dfs://new/external-data"));

        assertThat(
                        mapping.rewriteRequiredUnder(
                                "dfs://old/warehouse/table/data/bucket-0/file.orc",
                                "dfs://old/warehouse/table"))
                .isEqualTo("dfs://new/warehouse/table/data/bucket-0/file.orc");
        assertThat(mapping.rewriteRequired("dfs://old/warehouse/table/data/bucket-0/file.orc"))
                .isEqualTo("dfs://new/external-data/bucket-0/file.orc");
    }

    @Test
    public void testOptionalRewrite() {
        PathMapping mapping =
                PathMapping.parse(
                        Collections.singletonList("dfs://old/warehouse=dfs://new/warehouse"));

        assertThat(mapping.rewrite("dfs://old/warehouse/db/t/file.orc"))
                .contains("dfs://new/warehouse/db/t/file.orc");
        assertThat(mapping.rewrite("dfs://other/warehouse/db/t/file.orc")).isNotPresent();
    }

    @Test
    public void testPrefixMustMatchPathBoundary() {
        PathMapping mapping =
                PathMapping.parse(
                        Collections.singletonList(
                                "dfs://old/warehouse/table=dfs://new/warehouse/table"));

        assertThat(mapping.rewrite("dfs://old/warehouse/table/file.orc"))
                .contains("dfs://new/warehouse/table/file.orc");
        assertThat(mapping.rewrite("dfs://old/warehouse/table_backup/file.orc")).isNotPresent();
    }

    @Test
    public void testTrailingSlashIsNormalized() {
        PathMapping mapping =
                PathMapping.parse(
                        Collections.singletonList("dfs://old/warehouse/=dfs://new/warehouse/"));

        assertThat(mapping.rewriteRequired("dfs://old/warehouse/db/t/file.orc"))
                .isEqualTo("dfs://new/warehouse/db/t/file.orc");
    }

    @Test
    public void testEquivalentFileUriForms() {
        PathMapping mapping =
                PathMapping.parse(
                        Collections.singletonList("file:///tmp/source=file:///tmp/target"));

        assertThat(mapping.rewriteRequired("file:/tmp/source/data/file.parquet"))
                .isEqualTo("file:/tmp/target/data/file.parquet");
        assertThat(mapping.rewriteRequired("file:///tmp/source/data/file.parquet"))
                .isEqualTo("file:/tmp/target/data/file.parquet");
    }

    @Test
    public void testFileUriAndSchemeLessPathsAreDifferentForRewrite() {
        PathMapping filePrefix =
                PathMapping.parse(Collections.singletonList("file:/tmp/source=file:/tmp/target"));
        assertThat(filePrefix.rewrite("/tmp/source/data/file.parquet")).isNotPresent();

        PathMapping schemeLessPrefix =
                PathMapping.parse(Collections.singletonList("/tmp/source=/tmp/target"));
        assertThat(schemeLessPrefix.rewrite("file:/tmp/source/data/file.parquet")).isNotPresent();
    }

    @Test
    public void testMixedLocalFormsKeepIndependentPrefixes() {
        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList("file:/x=s3://target/shallow", "/x/y=s3://target/deep"));

        assertThat(mapping.rewriteRequired("file:/x/y/file.parquet"))
                .isEqualTo("s3://target/shallow/y/file.parquet");
        assertThat(mapping.rewriteRequired("/x/y/file.parquet"))
                .isEqualTo("s3://target/deep/file.parquet");
    }

    @Test
    public void testRootPrefixRewritePreservesPathBoundary() {
        PathMapping fileRoot =
                PathMapping.parse(Collections.singletonList("file:/=s3://bucket/target/"));
        assertThat(fileRoot.rewriteRequired("file:/warehouse/table/data/file.parquet"))
                .isEqualTo("s3://bucket/target/warehouse/table/data/file.parquet");

        PathMapping localRoot =
                PathMapping.parse(Collections.singletonList("/=s3://bucket/target"));
        assertThat(localRoot.rewriteRequired("/warehouse/table/data/file.parquet"))
                .isEqualTo("s3://bucket/target/warehouse/table/data/file.parquet");
    }

    @Test
    public void testUnmatchedRequiredPathFails() {
        PathMapping mapping =
                PathMapping.parse(
                        Collections.singletonList("dfs://old/warehouse=dfs://new/warehouse"));

        assertThatThrownBy(() -> mapping.rewriteRequired("dfs://other/warehouse/db/t/file.orc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No path mapping");
    }

    @Test
    public void testInvalidMappingFails() {
        assertThatThrownBy(() -> PathMapping.parse(Collections.singletonList("dfs://old")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source=target");

        assertThatThrownBy(
                        () ->
                                PathMapping.parse(
                                        Arrays.asList(
                                                "dfs://old=dfs://new1", "dfs://old=dfs://new2")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate");

        assertThatThrownBy(
                        () ->
                                PathMapping.parse(
                                        Collections.singletonList(
                                                "s3://user:secret@old/table=s3://new/table")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("user info")
                .hasMessageNotContaining("secret");

        assertThatThrownBy(
                        () ->
                                PathMapping.parse(
                                        Collections.singletonList("s3://user:secret@old/table")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source=target")
                .hasMessageNotContaining("secret");
    }

    @Test
    public void testRuntimeSourceUserInfoIsRedacted() {
        PathMapping mapping =
                PathMapping.parse(Collections.singletonList("s3://old/table=s3://new/table"));

        assertThatThrownBy(
                        () ->
                                mapping.rewriteRequired(
                                        "s3://runtime-user:runtime-secret@old/table/data"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("user info")
                .hasMessageNotContaining("runtime-secret");
    }

    @Test
    public void testEquivalentLocalPrefixesCannotOverlap() {
        assertThatThrownBy(
                        () ->
                                PathMapping.parse(
                                        Arrays.asList(
                                                "file:/tmp/source=file:/tmp/target",
                                                "/tmp/target=/tmp/other")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not overlap");

        assertThatThrownBy(
                        () ->
                                PathMapping.parse(
                                        Arrays.asList(
                                                "file:/tmp/source=file:/tmp/target-one",
                                                "/tmp/source=/tmp/target-two")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate path mapping source prefix");
    }

    @Test
    public void testNonLocalMappingRequiresAuthority() {
        assertThatThrownBy(
                        () ->
                                PathMapping.parse(
                                        Arrays.asList(
                                                "hdfs://source/a=hdfs:/clone",
                                                "hdfs://source/b=hdfs://namenode/clone")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Non-local path mapping prefix must include authority")
                .hasMessageContaining("hdfs:/clone");
    }

    @Test
    public void testOverlappingSourceAndTargetPrefixesFail() {
        assertThatThrownBy(
                        () ->
                                PathMapping.parse(
                                        Collections.singletonList(
                                                "dfs://cluster/table=dfs://cluster/table/clone")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not overlap");

        assertThatThrownBy(
                        () ->
                                PathMapping.parse(
                                        Arrays.asList(
                                                "dfs://old/a=dfs://new/a",
                                                "dfs://new=dfs://third")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not overlap");

        assertThatThrownBy(
                        () ->
                                PathMapping.parse(
                                        Collections.singletonList(
                                                "hdfs://NameNode/warehouse/table="
                                                        + "HDFS://namenode/warehouse/table/clone")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not overlap");
    }

    @Test
    public void testOverlappingTargetPrefixesFail() {
        assertThatThrownBy(
                        () ->
                                PathMapping.parse(
                                        Arrays.asList(
                                                "dfs://old/a=dfs://new/same",
                                                "dfs://old/b=dfs://new/same")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Target path mapping prefixes must not overlap");

        assertThatThrownBy(
                        () ->
                                PathMapping.parse(
                                        Arrays.asList(
                                                "dfs://old/a=dfs://new/root",
                                                "dfs://old/b=dfs://new/root/nested")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Target path mapping prefixes must not overlap");
    }

    @Test
    public void testRewriteAllRequired() {
        PathMapping mapping =
                PathMapping.parse(
                        Collections.singletonList("dfs://old/warehouse=dfs://new/warehouse"));

        Map<String, String> result =
                mapping.rewriteAllRequired(
                        Arrays.asList(
                                "dfs://old/warehouse/db/t/file1.orc",
                                "dfs://old/warehouse/db/t/file2.orc"));

        assertThat(result)
                .containsEntry(
                        "dfs://old/warehouse/db/t/file1.orc", "dfs://new/warehouse/db/t/file1.orc")
                .containsEntry(
                        "dfs://old/warehouse/db/t/file2.orc", "dfs://new/warehouse/db/t/file2.orc");
    }

    @Test
    public void testIdentityIsNormalizedAndOrderIndependent() {
        PathMapping first =
                PathMapping.parse(
                        Arrays.asList(
                                "file:///tmp/source=file:///tmp/target",
                                "dfs://old/external/=dfs://new/external/"));
        PathMapping second =
                PathMapping.parse(
                        Arrays.asList(
                                "dfs://old/external=dfs://new/external",
                                "file:/tmp/source=file:/tmp/target"));

        assertThat(first.identity()).isEqualTo(second.identity());
    }
}
