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

package org.apache.paimon.flink.kafka;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.AbstractFlinkTableFactory;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.flink.sink.FlinkTableSink;
import org.apache.paimon.flink.source.DataTableSource;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;

/** UT about {@link KafkaLogStoreFactory}. */
public class KafkaLogStoreFactoryTest {

    @ParameterizedTest
    @EnumSource(CoreOptions.StartupMode.class)
    public void testCreateKafkaLogStoreFactoryTimestamp(CoreOptions.StartupMode startupMode) {
        String now = String.valueOf(System.currentTimeMillis());
        Consumer<Map<String, String>> setter =
                (options) -> options.put(SCAN_TIMESTAMP_MILLIS.key(), now);
        testCreateKafkaLogStoreFactory(startupMode, setter);
    }

    @ParameterizedTest
    @EnumSource(CoreOptions.StartupMode.class)
    public void testCreateKafkaLogStoreFactoryTimestampStr(CoreOptions.StartupMode startupMode) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String timestampString = LocalDateTime.now().format(formatter);
        Consumer<Map<String, String>> setter =
                (options) -> options.put(SCAN_TIMESTAMP.key(), timestampString);
        testCreateKafkaLogStoreFactory(startupMode, setter);
    }

    private static void testCreateKafkaLogStoreFactory(
            CoreOptions.StartupMode startupMode, Consumer<Map<String, String>> optionsSetter) {
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(FlinkConnectorOptions.LOG_SYSTEM.key(), "kafka");
        dynamicOptions.put(SCAN_MODE.key(), startupMode.toString());
        if (startupMode == CoreOptions.StartupMode.FROM_SNAPSHOT
                || startupMode == CoreOptions.StartupMode.FROM_SNAPSHOT_FULL) {
            dynamicOptions.put(SCAN_SNAPSHOT_ID.key(), "1");
        } else if (startupMode == CoreOptions.StartupMode.FROM_TIMESTAMP) {
            optionsSetter.accept(dynamicOptions);
        }
        dynamicOptions.put(SCAN_MODE.key(), startupMode.toString());
        DynamicTableFactory.Context context =
                KafkaLogTestUtils.testContext(
                        "table",
                        "",
                        CoreOptions.LogChangelogMode.AUTO,
                        CoreOptions.LogConsistency.TRANSACTIONAL,
                        RowType.of(new IntType(), new IntType()),
                        new int[] {0},
                        dynamicOptions);

        try {
            Optional<LogStoreTableFactory> optional =
                    AbstractFlinkTableFactory.createOptionalLogStoreFactory(context);
            assertThat(startupMode)
                    .isNotIn(
                            CoreOptions.StartupMode.FROM_SNAPSHOT,
                            CoreOptions.StartupMode.FROM_SNAPSHOT_FULL);
            assertThat(optional.isPresent()).isTrue();
            assertThat(optional.get()).isInstanceOf(KafkaLogStoreFactory.class);
        } catch (ValidationException e) {
            assertThat(startupMode)
                    .isIn(
                            CoreOptions.StartupMode.FROM_SNAPSHOT,
                            CoreOptions.StartupMode.FROM_SNAPSHOT_FULL);
        }
    }

    @Test
    public void testInputChangelogProducerWithKafkaLog(@TempDir java.nio.file.Path temp)
            throws Exception {
        Options options = new Options();
        options.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.INPUT);

        Path path = new Path(temp.toUri().toString());
        new SchemaManager(LocalFileIO.create(), path)
                .createTable(
                        new Schema(
                                org.apache.paimon.types.RowType.of(
                                                new org.apache.paimon.types.IntType(),
                                                new org.apache.paimon.types.IntType())
                                        .getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("f0"),
                                options.toMap(),
                                ""));
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), path);

        ObjectIdentifier identifier = ObjectIdentifier.of("c", "d", "t");
        DataTableSource source =
                new DataTableSource(identifier, table, true, null, new KafkaLogStoreFactory());
        assertThat(source.getChangelogMode()).isEqualTo(ChangelogMode.upsert());

        FlinkTableSink sink = new FlinkTableSink(identifier, table, null, null);
        assertThat(sink.getChangelogMode(ChangelogMode.all())).isEqualTo(ChangelogMode.all());
    }
}
