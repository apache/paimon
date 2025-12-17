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

package org.apache.paimon.partition;

import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Partition} JSON serialization. */
class PartitionTest {

    @Test
    void testJsonSerializationWithNullValues() {
        Map<String, String> spec = Collections.singletonMap("pt", "1");
        Partition partition =
                new Partition(
                        spec,
                        100L, // recordCount
                        1024L, // fileSizeInBytes
                        2L, // fileCount
                        System.currentTimeMillis(), // lastFileCreationTime
                        false, // done
                        null, // createdAt
                        null, // createdBy
                        null, // updatedAt
                        null, // updatedBy
                        null); // options

        String json = JsonSerdeUtil.toFlatJson(partition);

        assertThat(json).doesNotContain("createdAt");
        assertThat(json).doesNotContain("createdBy");
        assertThat(json).doesNotContain("updatedAt");
        assertThat(json).doesNotContain("updatedBy");
        assertThat(json).doesNotContain("options");

        assertThat(json).contains("done");
        assertThat(json).contains("recordCount");
    }

    @Test
    void testJsonSerializationWithNonNullValues() {
        Map<String, String> spec = Collections.singletonMap("pt", "1");
        Partition partition =
                new Partition(
                        spec,
                        100L,
                        1024L,
                        2L,
                        System.currentTimeMillis(),
                        true,
                        1234567890L, // createdAt
                        "user1", // createdBy
                        1234567900L, // updatedAt
                        "user2", // updatedBy
                        Collections.singletonMap("key", "value")); // options

        String json = JsonSerdeUtil.toFlatJson(partition);

        assertThat(json).contains("createdAt");
        assertThat(json).contains("createdBy");
        assertThat(json).contains("updatedAt");
        assertThat(json).contains("updatedBy");
        assertThat(json).contains("options");
    }
}
