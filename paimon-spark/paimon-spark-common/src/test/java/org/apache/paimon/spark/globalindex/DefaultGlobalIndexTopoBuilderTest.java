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

package org.apache.paimon.spark.globalindex;

import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_BUILD_MAX_PARALLELISM;
import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_ROW_COUNT_PER_SHARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultGlobalIndexTopoBuilder}. */
public class DefaultGlobalIndexTopoBuilderTest {

    @Test
    void testRowsPerShardUsesMergedBuildOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(GLOBAL_INDEX_ROW_COUNT_PER_SHARD.key(), "1000");
        Map<String, String> buildOptions = new HashMap<>();
        buildOptions.put(GLOBAL_INDEX_ROW_COUNT_PER_SHARD.key(), "25");

        assertThat(
                        DefaultGlobalIndexTopoBuilder.rowsPerShard(
                                new Options(tableOptions, buildOptions)))
                .isEqualTo(25L);
    }

    @Test
    void testParallelismUsesBuildMaxParallelism() {
        Options options =
                new Options(
                        Collections.singletonMap(GLOBAL_INDEX_BUILD_MAX_PARALLELISM.key(), "2"));

        assertThat(DefaultGlobalIndexTopoBuilder.parallelism(5, options)).isEqualTo(2);
        assertThat(DefaultGlobalIndexTopoBuilder.parallelism(1, options)).isEqualTo(1);
    }

    @Test
    void testRowsPerShardMustBePositive() {
        Options options =
                new Options(Collections.singletonMap(GLOBAL_INDEX_ROW_COUNT_PER_SHARD.key(), "0"));

        assertThatThrownBy(() -> DefaultGlobalIndexTopoBuilder.rowsPerShard(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Option 'global-index.row-count-per-shard' must be greater than 0.");
    }

    @Test
    void testMaxParallelismMustBePositive() {
        Options options =
                new Options(
                        Collections.singletonMap(GLOBAL_INDEX_BUILD_MAX_PARALLELISM.key(), "0"));

        assertThatThrownBy(() -> DefaultGlobalIndexTopoBuilder.parallelism(5, options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Option 'global-index.build.max-parallelism' must be greater than 0.");
    }
}
