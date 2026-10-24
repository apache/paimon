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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CoreOptions#COMPACTION_TASK_THREADS}. */
class CoreOptionsCompactionTaskThreadsTest {

    @Test
    void testDefaultIsSingleThreadMode() {
        CoreOptions options = new CoreOptions(new Options());
        assertThat(options.compactionTaskExecutorMode())
                .isEqualTo(CompactionTaskExecutorMode.SINGLE);
        assertThat(options.compactionTaskThreads()).isEqualTo(1);
    }

    @Test
    void testFixedPoolMode() {
        Options options = new Options();
        options.set(CoreOptions.COMPACTION_TASK_THREADS, 3);
        CoreOptions coreOptions = new CoreOptions(options);
        assertThat(coreOptions.compactionTaskExecutorMode())
                .isEqualTo(CompactionTaskExecutorMode.FIXED_POOL);
        assertThat(coreOptions.compactionTaskThreads()).isEqualTo(3);
    }

    @Test
    void testPerBucketMode() {
        Options options = new Options();
        options.set(CoreOptions.COMPACTION_TASK_THREADS, -1);
        CoreOptions coreOptions = new CoreOptions(options);
        assertThat(coreOptions.compactionTaskExecutorMode())
                .isEqualTo(CompactionTaskExecutorMode.PER_BUCKET);
    }

    @Test
    void testRejectZeroAndOtherNegativeValues() {
        assertInvalid(0);
        assertInvalid(-2);
        assertInvalid(-100);
    }

    private static void assertInvalid(int threads) {
        Options options = new Options();
        options.set(CoreOptions.COMPACTION_TASK_THREADS, threads);
        CoreOptions coreOptions = new CoreOptions(options);
        assertThatThrownBy(coreOptions::compactionTaskExecutorMode)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(CoreOptions.COMPACTION_TASK_THREADS.key());
    }
}
