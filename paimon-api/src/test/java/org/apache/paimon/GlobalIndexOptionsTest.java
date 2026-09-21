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

import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for global index configuration compatibility. */
class GlobalIndexOptionsTest {

    @Test
    void testRowCountPerFileDefault() {
        assertThat(new CoreOptions(new Options()).globalIndexRowCountPerShard())
                .isEqualTo(25_000_000L);
    }

    @Test
    void testRowCountPerFileFallsBackToLegacyKey() {
        Options options = new Options();
        options.setString("global-index.row-count-per-shard", "100000");

        assertThat(new CoreOptions(options).globalIndexRowCountPerShard()).isEqualTo(100_000L);
    }

    @Test
    void testRowCountPerFileTakesPrecedenceOverLegacyKey() {
        Options options = new Options();
        options.setString("global-index.row-count-per-file", "2500");
        CoreOptions coreOptions = new CoreOptions(options);
        assertThat(coreOptions.globalIndexRowCountPerShard()).isEqualTo(2500L);

        options.setString("global-index.row-count-per-shard", "100000");
        assertThat(coreOptions.globalIndexRowCountPerShard()).isEqualTo(2500L);
    }
}
