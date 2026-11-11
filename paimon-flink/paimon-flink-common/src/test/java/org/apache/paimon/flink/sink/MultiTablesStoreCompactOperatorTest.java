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

package org.apache.paimon.flink.sink;

import org.apache.paimon.options.Options;

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MultiTablesStoreCompactOperator}. */
public class MultiTablesStoreCompactOperatorTest {

    @Test
    public void testCloseAfterInitializeStateFailure() throws Exception {
        MultiTablesStoreCompactOperator.Factory operatorFactory =
                new MultiTablesStoreCompactOperator.Factory(
                        () -> {
                            throw new RuntimeException("Failed to load catalog");
                        },
                        "10086",
                        new CheckpointConfig(),
                        true,
                        false,
                        false,
                        new Options());
        try (OneInputStreamOperatorTestHarness<RowData, MultiTableCommittable> harness =
                new OneInputStreamOperatorTestHarness<>(operatorFactory)) {
            harness.setup();
            assertThatThrownBy(harness::initializeEmptyState)
                    .hasMessageContaining("Failed to load catalog");

            assertThatCode(() -> harness.getOperator().close()).doesNotThrowAnyException();
        }
    }
}
