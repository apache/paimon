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

package org.apache.paimon;

import org.apache.paimon.CoreOptions.DataEvolutionMetadataMemoryMode;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Explicit modes for manually enabled metadata memory optimization. */
class DataEvolutionMetadataMemoryOptionsTest {

    private static final String MODE = "data-evolution.metadata-memory-optimization.mode";

    @Test
    void testDefaultRemainsOff() {
        CoreOptions core = new CoreOptions(deOptions());
        assertThat(core.dataEvolutionMetadataMemoryMode())
                .isEqualTo(DataEvolutionMetadataMemoryMode.OFF);
        assertThat(core.dataEvolutionMetadataMemoryOptimizationEnabled()).isFalse();
        assertThat(core.dataEvolutionMetadataStreamingEnabled()).isFalse();
    }

    @Test
    void testExplicitModes() {
        for (DataEvolutionMetadataMemoryMode mode : DataEvolutionMetadataMemoryMode.values()) {
            Options options = deOptions();
            options.setString(MODE, mode.toString());
            CoreOptions core = new CoreOptions(options);
            assertThat(core.dataEvolutionMetadataMemoryMode()).isEqualTo(mode);
            assertThat(core.dataEvolutionMetadataMemoryOptimizationEnabled())
                    .isEqualTo(mode != DataEvolutionMetadataMemoryMode.OFF);
            assertThat(core.dataEvolutionMetadataStreamingEnabled())
                    .isEqualTo(mode == DataEvolutionMetadataMemoryMode.STREAMING);
        }
    }

    @Test
    void testModeParsingUsesExistingCaseInsensitiveEnumRules() {
        Options options = deOptions();
        options.setString(MODE, "StReAmInG");
        CoreOptions core = new CoreOptions(options);
        assertThat(core.dataEvolutionMetadataMemoryMode())
                .isEqualTo(DataEvolutionMetadataMemoryMode.STREAMING);
        assertThat(core.dataEvolutionMetadataStreamingEnabled()).isTrue();
    }

    @Test
    void testInvalidModeDoesNotSilentlyFallBack() {
        Options options = deOptions();
        options.setString(MODE, "automatic");
        assertThatThrownBy(() -> new CoreOptions(options).dataEvolutionMetadataMemoryMode())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(MODE)
                .hasMessageContaining("automatic")
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("off")
                .hasMessageContaining("lightweight")
                .hasMessageContaining("streaming");
    }

    @Test
    void testNonDataEvolutionTablesDoNotEnableEitherPath() {
        for (DataEvolutionMetadataMemoryMode mode : DataEvolutionMetadataMemoryMode.values()) {
            Options options = new Options();
            options.setString(MODE, mode.toString());
            CoreOptions core = new CoreOptions(options);
            assertThat(core.dataEvolutionMetadataMemoryMode())
                    .isEqualTo(DataEvolutionMetadataMemoryMode.OFF);
            assertThat(core.dataEvolutionMetadataMemoryOptimizationEnabled()).isFalse();
            assertThat(core.dataEvolutionMetadataStreamingEnabled()).isFalse();
        }
    }

    @Test
    void testReplayDiskBudgetHasAValidatedExplicitLimit() {
        Options options = deOptions();
        CoreOptions core = new CoreOptions(options);
        assertThat(core.dataEvolutionMetadataMaxDiskSize()).isEqualTo(1024L * 1024 * 1024);
        String key = "data-evolution.metadata-memory-optimization.max-disk-size";
        options.setString(key, "8 b");
        assertThat(core.dataEvolutionMetadataMaxDiskSize()).isEqualTo(8L);
        for (String invalid : new String[] {"0 b", "7 b"}) {
            options.setString(key, invalid);
            assertThatThrownBy(core::dataEvolutionMetadataMaxDiskSize)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(key);
        }
    }

    private static Options deOptions() {
        Options options = new Options();
        options.set(CoreOptions.DATA_EVOLUTION_ENABLED, true);
        return options;
    }
}
