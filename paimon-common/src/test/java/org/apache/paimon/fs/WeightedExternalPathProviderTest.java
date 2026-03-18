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

package org.apache.paimon.fs;

import org.apache.paimon.CoreOptions;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link WeightedExternalPathProvider}. */
public class WeightedExternalPathProviderTest {

    @Test
    public void testEqualWeights() {
        int fileNum = 30000;
        int[] weights = {10, 10, 10};
        Map<String, Integer> pathCounts = generatePaths(fileNum, weights);

        int expectedCount = fileNum / 3;
        for (int count : pathCounts.values()) {
            assertThat(count).isBetween(expectedCount - 1000, expectedCount + 1000);
        }
    }

    @Test
    public void testDifferentWeights() {
        int[] weights = {10, 5, 15};
        int fileNum = 30000;
        Map<String, Integer> pathCounts = generatePaths(fileNum, weights);

        int totalWeight = 30;
        assertThat(pathCounts.get("s3://bucket1/data"))
                .isBetween(
                        (int) (fileNum * 10.0 / totalWeight) - 1000,
                        (int) (fileNum * 10.0 / totalWeight) + 1000);
        assertThat(pathCounts.get("oss://bucket2/data"))
                .isBetween(
                        (int) (fileNum * 5.0 / totalWeight) - 1000,
                        (int) (fileNum * 5.0 / totalWeight) + 1000);
        assertThat(pathCounts.get("hdfs://namenode/data"))
                .isBetween(
                        (int) (fileNum * 15.0 / totalWeight) - 1000,
                        (int) (fileNum * 15.0 / totalWeight) + 1000);
    }

    @Test
    public void testSinglePath() {
        List<Path> paths = new ArrayList<>();
        paths.add(new Path("s3://bucket1/data"));

        int[] weights = {10};

        Path relativeBucketPath = new Path("bucket-0");
        WeightedExternalPathProvider provider =
                new WeightedExternalPathProvider(paths, relativeBucketPath, weights);

        for (int fileNum = 0; fileNum < 1000; fileNum++) {
            Path selectedPath = provider.getNextExternalDataPath("file-" + fileNum + ".parquet");
            assertThat(selectedPath.toString())
                    .contains("s3://bucket1/data/bucket-0/file-" + fileNum + ".parquet");
        }
    }

    @Test
    public void testMissingWeight() {
        List<Path> paths = new ArrayList<>();
        paths.add(new Path("s3://bucket1/data"));
        paths.add(new Path("oss://bucket2/data"));

        int[] weights = {10};
        // Missing weight for oss://bucket2/data

        Path relativeBucketPath = new Path("bucket-0");

        assertThatThrownBy(
                        () -> new WeightedExternalPathProvider(paths, relativeBucketPath, weights))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "The number of external paths and weights should be the same. Paths: 2, Weights: 1");
    }

    @Test
    public void testPathConstruction() {
        List<Path> paths = new ArrayList<>();
        paths.add(new Path("s3://bucket1/data"));

        int[] weights = {10};

        Path relativeBucketPath = new Path("bucket-0");
        WeightedExternalPathProvider provider =
                new WeightedExternalPathProvider(paths, relativeBucketPath, weights);

        Path selectedPath = provider.getNextExternalDataPath("test-file.parquet");
        assertThat(selectedPath.toString())
                .isEqualTo("s3://bucket1/data/bucket-0/test-file.parquet");
    }

    @Test
    public void testCreateExternalPathProvider() {
        ExternalPathProvider provider1 =
                ExternalPathProvider.create(
                        CoreOptions.ExternalPathStrategy.WEIGHTED,
                        Arrays.asList(new Path("oss://path1"), new Path("oss://path2")),
                        new Path("bucket-0"),
                        null);
        assertThat(provider1).isInstanceOf(RoundRobinExternalPathProvider.class);

        ExternalPathProvider provider2 =
                ExternalPathProvider.create(
                        CoreOptions.ExternalPathStrategy.WEIGHTED,
                        Collections.singletonList(new Path("oss://path1")),
                        new Path("bucket-0"),
                        new int[] {10});
        assertThat(provider2).isInstanceOf(RoundRobinExternalPathProvider.class);

        ExternalPathProvider provider3 =
                ExternalPathProvider.create(
                        CoreOptions.ExternalPathStrategy.WEIGHTED,
                        Arrays.asList(new Path("oss://path1"), new Path("oss://path2")),
                        new Path("bucket-0"),
                        new int[] {10, 20});
        assertThat(provider3).isInstanceOf(WeightedExternalPathProvider.class);
    }

    private Map<String, Integer> generatePaths(int fileNum, int[] weights) {
        List<Path> paths = new ArrayList<>();
        paths.add(new Path("s3://bucket1/data"));
        paths.add(new Path("oss://bucket2/data"));
        paths.add(new Path("hdfs://namenode/data"));

        Path relativeBucketPath = new Path("bucket-0");
        WeightedExternalPathProvider provider =
                new WeightedExternalPathProvider(paths, relativeBucketPath, weights);

        Map<String, Integer> pathCounts = new HashMap<>();
        for (int i = 0; i < fileNum; i++) {
            Path selectedPath = provider.getNextExternalDataPath("file-" + i + ".parquet");
            String basePath = selectedPath.getParent().getParent().toString();
            pathCounts.put(basePath, pathCounts.getOrDefault(basePath, 0) + 1);
        }
        return pathCounts;
    }
}
