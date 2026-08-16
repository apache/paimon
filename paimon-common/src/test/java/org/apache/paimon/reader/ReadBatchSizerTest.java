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

package org.apache.paimon.reader;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ReadBatchSizer}. */
class ReadBatchSizerTest {

    @Test
    void testUpdateAndClearBatchSize() {
        ReadBatchSizer sizer = new ReadBatchSizer();

        assertThat(sizer.batchSize()).isEmpty();

        sizer.setBatchSize(512);
        assertThat(sizer.batchSize()).hasValue(512);

        sizer.clearBatchSize();
        assertThat(sizer.batchSize()).isEmpty();
    }

    @Test
    void testRejectInvalidBatchSizes() {
        ReadBatchSizer sizer = new ReadBatchSizer();
        assertThatThrownBy(() -> sizer.setBatchSize(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sizer.setBatchSize(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
