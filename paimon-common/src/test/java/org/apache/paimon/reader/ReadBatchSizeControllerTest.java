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

/** Tests for {@link ReadBatchSizeController}. */
class ReadBatchSizeControllerTest {

    @Test
    void testUpdateRequestedBatchSizeWithinMaximum() {
        ReadBatchSizeController controller = new ReadBatchSizeController(1024, 64);

        assertThat(controller.maxBatchSize()).isEqualTo(1024);
        assertThat(controller.requestedBatchSize()).isEqualTo(64);

        controller.setRequestedBatchSize(512);
        assertThat(controller.requestedBatchSize()).isEqualTo(512);
    }

    @Test
    void testRejectInvalidBatchSizes() {
        assertThatThrownBy(() -> new ReadBatchSizeController(0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ReadBatchSizeController(1024, 1025))
                .isInstanceOf(IllegalArgumentException.class);

        ReadBatchSizeController controller = new ReadBatchSizeController(1024, 64);
        assertThatThrownBy(() -> controller.setRequestedBatchSize(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> controller.setRequestedBatchSize(1025))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
