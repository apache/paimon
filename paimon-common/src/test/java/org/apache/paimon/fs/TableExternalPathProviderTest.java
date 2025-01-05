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

import org.apache.paimon.CoreOptions.ExternalPathStrategy;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TableExternalPathProvider}. */
public class TableExternalPathProviderTest {
    private TableExternalPathProvider provider;

    @BeforeEach
    public void setUp() {
        provider =
                new TableExternalPathProvider(
                        "oss://bucket1/path1,s3://bucket2/path2",
                        ExternalPathStrategy.ROUND_ROBIN,
                        null);
    }

    @Test
    public void testInitExternalPaths() {
        assertThat(provider.externalPathExists()).isTrue();
        assertThat(provider.getExternalPathsMap().size()).isEqualTo(2);
        assertThat(provider.getExternalPathsList().size()).isEqualTo(2);
    }

    @Test
    public void testGetNextExternalPathRoundRobinSimple() {
        String path1 = "s3://bucket2/path2";
        String path2 = "oss://bucket1/path1";
        List<String> expectedPaths = new ArrayList<String>();
        expectedPaths.add(path1);
        expectedPaths.add(path2);
        String externalPaths = path1 + "," + path2;
        provider =
                new TableExternalPathProvider(
                        externalPaths, ExternalPathStrategy.ROUND_ROBIN, null);

        // Collect the paths returned by getNextExternalPath
        List<String> actualPaths = new ArrayList<>();
        int expectIndex = 0;
        for (int i = 0; i < 6; i++) { // Collect more paths to ensure all are covered
            Optional<Path> path = provider.getNextExternalPath();
            assertThat(path.isPresent()).isTrue();
            actualPaths.add(path.get().toString());
            if (i == 0) {
                if (path.get().toString().equals(path1)) {
                    expectIndex = 0;
                } else if (path.get().toString().equals(path2)) {
                    expectIndex = 1;
                }
            }

            expectIndex = (expectIndex) % expectedPaths.size();
            assertThat(path.get().toString().equals(expectedPaths.get(expectIndex))).isTrue();
            expectIndex++;
        }

        // Check that all expected paths are present in the actual paths
        for (String expectedPath : expectedPaths) {
            assertThat(actualPaths).contains(expectedPath);
        }
    }

    @Test
    public void testGetNextExternalPathRoundRobinComplex() {
        List<String> expectedPathsList = new ArrayList<String>();
        for (int i = 0; i < 20; i++) {
            if (i % 2 == 0) {
                expectedPathsList.add("oss://bucket1/path" + i);
            } else {
                expectedPathsList.add("s3://bucket2/path" + i);
            }
        }
        String externalPaths = String.join(",", expectedPathsList);
        provider =
                new TableExternalPathProvider(
                        externalPaths, ExternalPathStrategy.ROUND_ROBIN, null);

        // Collect the paths returned by getNextExternalPath
        List<String> actualPaths = new ArrayList<>();
        int expectIndex = 0;
        for (int i = 0; i < 40; i++) { // Collect more paths to ensure all are covered
            Optional<Path> path = provider.getNextExternalPath();
            assertThat(path.isPresent()).isTrue();
            actualPaths.add(path.get().toString());
            if (i == 0) {
                for (int j = 0; j < expectedPathsList.size(); j++) {
                    if (path.get().toString().equals(expectedPathsList.get(j))) {
                        expectIndex = j;
                        break;
                    }
                }
            }
            expectIndex = (expectIndex) % expectedPathsList.size();
            assertThat(path.get().toString().equals(expectedPathsList.get(expectIndex))).isTrue();
            expectIndex++;
        }

        // Check that all expected paths are present in the actual paths
        for (String expectedPath : expectedPathsList) {
            assertThat(actualPaths).contains(expectedPath);
        }
    }

    @Test
    public void testGetNextExternalPathSpecificFS() {
        provider =
                new TableExternalPathProvider(
                        "oss://bucket1/path1,s3://bucket2/path2",
                        ExternalPathStrategy.SPECIFIC_FS,
                        "OSS");

        Optional<Path> path = provider.getNextExternalPath();
        assertThat(path.isPresent()).isTrue();
        assertThat(path.get().toString()).isEqualTo("oss://bucket1/path1");
    }

    @Test
    public void testGetNextExternalPathNone() {
        provider =
                new TableExternalPathProvider(
                        "oss://bucket1/path1,s3://bucket2/path2", ExternalPathStrategy.NONE, "OSS");

        Optional<Path> path = provider.getNextExternalPath();
        assertThat(path.isPresent()).isFalse();
    }

    @Test
    public void testUnsupportedExternalPath() {
        Assertions.assertThatThrownBy(
                        () -> {
                            new TableExternalPathProvider(
                                    "hdfs://bucket1/path1",
                                    ExternalPathStrategy.SPECIFIC_FS,
                                    "oss");
                        })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testUnsupportedExternalSpecificFS() {
        assertThatThrownBy(
                        () -> {
                            provider =
                                    new TableExternalPathProvider(
                                            "oss://bucket1/path1",
                                            ExternalPathStrategy.SPECIFIC_FS,
                                            "S3");
                        })
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "external specific fs not found: s3"));
    }

    @Test
    public void testExternalSpecificFSNull() {
        Assertions.assertThatThrownBy(
                        () -> {
                            new TableExternalPathProvider(
                                    "oss://bucket1/path1", ExternalPathStrategy.SPECIFIC_FS, null);
                        })
                .isInstanceOf(IllegalArgumentException.class);
    }
}
