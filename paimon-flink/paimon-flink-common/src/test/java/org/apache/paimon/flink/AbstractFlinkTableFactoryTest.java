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

package org.apache.paimon.flink;

import org.apache.paimon.flink.kafka.KafkaLogStoreFactory;
import org.apache.paimon.flink.kafka.KafkaLogTestUtils;
import org.apache.paimon.flink.log.LogStoreTableFactory;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.LogChangelogMode;
import static org.apache.paimon.CoreOptions.LogConsistency;
import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;
import static org.apache.paimon.CoreOptions.StartupMode;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AbstractFlinkTableFactory}. */
public class AbstractFlinkTableFactoryTest {

    @Test
    public void testSchemaEquals() {
        innerTest(RowType.of(false), RowType.of(true), true);
        innerTest(RowType.of(false), RowType.of(false, new VarCharType()), false);
        innerTest(
                RowType.of(new LogicalType[] {new VarCharType()}, new String[] {"foo"}),
                RowType.of(new VarCharType()),
                false);
        innerTest(
                new RowType(
                        true,
                        Arrays.asList(
                                new RowType.RowField("foo", new VarCharType(), "comment about foo"),
                                new RowType.RowField("bar", new IntType()))),
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField("foo", new VarCharType()),
                                new RowType.RowField("bar", new IntType(), "comment about bar"))),
                true);
    }

    @ParameterizedTest
    @EnumSource(StartupMode.class)
    public void testCreateKafkaLogStoreFactory(StartupMode startupMode) {
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(FlinkConnectorOptions.LOG_SYSTEM.key(), "kafka");
        dynamicOptions.put(SCAN_MODE.key(), startupMode.toString());
        if (startupMode == StartupMode.FROM_SNAPSHOT
                || startupMode == StartupMode.FROM_SNAPSHOT_FULL) {
            dynamicOptions.put(SCAN_SNAPSHOT_ID.key(), "1");
        } else if (startupMode == StartupMode.FROM_TIMESTAMP) {
            dynamicOptions.put(
                    SCAN_TIMESTAMP_MILLIS.key(), String.valueOf(System.currentTimeMillis()));
        }
        dynamicOptions.put(SCAN_MODE.key(), startupMode.toString());
        DynamicTableFactory.Context context =
                KafkaLogTestUtils.testContext(
                        "table",
                        "",
                        LogChangelogMode.AUTO,
                        LogConsistency.TRANSACTIONAL,
                        RowType.of(new IntType(), new IntType()),
                        new int[] {0},
                        dynamicOptions);

        try {
            Optional<LogStoreTableFactory> optional =
                    AbstractFlinkTableFactory.createOptionalLogStoreFactory(context);
            assertThat(startupMode)
                    .isNotIn(StartupMode.FROM_SNAPSHOT, StartupMode.FROM_SNAPSHOT_FULL);
            assertThat(optional.isPresent()).isTrue();
            assertThat(optional.get()).isInstanceOf(KafkaLogStoreFactory.class);
        } catch (ValidationException e) {
            assertThat(startupMode).isIn(StartupMode.FROM_SNAPSHOT, StartupMode.FROM_SNAPSHOT_FULL);
        }
    }

    private void innerTest(RowType r1, RowType r2, boolean expectEquals) {
        assertThat(AbstractFlinkTableFactory.schemaEquals(r1, r2)).isEqualTo(expectEquals);
    }
}
