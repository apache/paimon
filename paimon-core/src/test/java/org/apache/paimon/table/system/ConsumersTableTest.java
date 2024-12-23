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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link ConsumersTable}. */
public class ConsumersTableTest extends TableTestBase {

    private static final String tableName = "MyTable";

    private ConsumerManager manager;
    private ConsumersTable consumersTable;

    @BeforeEach
    public void before() throws Exception {
        Identifier identifier = identifier(tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("product_id", DataTypes.INT())
                        .column("price", DataTypes.INT())
                        .column("sales", DataTypes.INT())
                        .primaryKey("product_id")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                        .option(CoreOptions.BUCKET.key(), "2")
                        .build();
        catalog.createTable(identifier, schema, true);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        manager = new ConsumerManager(table.fileIO(), table.tableDataPath());
        manager.resetConsumer("id1", new Consumer(5));
        manager.resetConsumer("id2", new Consumer(6));
        consumersTable = (ConsumersTable) catalog.getTable(identifier(tableName + "$consumers"));
    }

    @Test
    public void testPartitionRecordCount() throws Exception {
        List<InternalRow> expectRow = getExpectedResult();
        List<InternalRow> result = read(consumersTable);
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectRow);
    }

    private List<InternalRow> getExpectedResult() throws IOException {
        Map<String, Long> consumers = manager.consumers();
        return consumers.entrySet().stream()
                .map(
                        entry ->
                                GenericRow.of(
                                        BinaryString.fromString(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList());
    }
}
