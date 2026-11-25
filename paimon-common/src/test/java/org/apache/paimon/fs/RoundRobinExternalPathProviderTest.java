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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RoundRobinExternalPathProvider}. */
public class RoundRobinExternalPathProviderTest {

    @Test
    public void testWithoutRelativeBucketPath() {
        RoundRobinExternalPathProvider provider =
                new RoundRobinExternalPathProvider(
                        Arrays.asList(
                                new Path("s3://bucket-a/index"), new Path("s3://bucket-b/index")),
                        null);

        Path first = provider.getNextExternalDataPath("index-file");
        Path second = provider.getNextExternalDataPath("index-file");
        assertThat(Arrays.asList(first, second))
                .containsExactlyInAnyOrder(
                        new Path("s3://bucket-a/index/index-file"),
                        new Path("s3://bucket-b/index/index-file"));
        assertThat(provider.getNextExternalDataPath("index-file")).isEqualTo(first);
        assertThat(provider.getNextExternalDataPath("index-file")).isEqualTo(second);
    }

    @Test
    public void testWithRelativeBucketPath() {
        RoundRobinExternalPathProvider provider =
                new RoundRobinExternalPathProvider(
                        Collections.singletonList(new Path("s3://bucket-a/data")),
                        new Path("dt=2026-11-18/bucket-3"));

        assertThat(provider.getNextExternalDataPath("data-file.parquet"))
                .isEqualTo(new Path("s3://bucket-a/data/dt=2026-11-18/bucket-3/data-file.parquet"));
    }
}
