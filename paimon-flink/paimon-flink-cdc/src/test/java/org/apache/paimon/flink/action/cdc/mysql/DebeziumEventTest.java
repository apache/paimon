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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.flink.action.cdc.mysql.format.DebeziumEvent;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.relational.history.TableChanges;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DebeziumEvent}. */
public class DebeziumEventTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    public void before() {
        objectMapper = new ObjectMapper();
        objectMapper
                .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Test
    public void testDeserialize() throws IOException {
        String json =
                "{\n"
                        + "    \"schema\":{\n"
                        + "        \"type\":\"struct\",\n"
                        + "        \"fields\":[\n"
                        + "            {\n"
                        + "                \"type\":\"struct\",\n"
                        + "                \"fields\":[\n"
                        + "                    {\n"
                        + "                        \"type\":\"string\",\n"
                        + "                        \"optional\":false,\n"
                        + "                        \"field\":\"version\"\n"
                        + "                    },\n"
                        + "                    {\n"
                        + "                        \"type\":\"string\",\n"
                        + "                        \"optional\":false,\n"
                        + "                        \"field\":\"connector\"\n"
                        + "                    },\n"
                        + "                    {\n"
                        + "                        \"type\":\"string\",\n"
                        + "                        \"optional\":false,\n"
                        + "                        \"field\":\"name\"\n"
                        + "                    },\n"
                        + "                    {\n"
                        + "                        \"type\":\"int64\",\n"
                        + "                        \"optional\":false,\n"
                        + "                        \"field\":\"ts_ms\"\n"
                        + "                    },\n"
                        + "                    {\n"
                        + "                        \"type\":\"string\",\n"
                        + "                        \"optional\":true,\n"
                        + "                        \"name\":\"io.debezium.data.Enum\",\n"
                        + "                        \"version\":1,\n"
                        + "                        \"parameters\":{\n"
                        + "                            \"allowed\":\"true,last,false\"\n"
                        + "                        },\n"
                        + "                        \"default\":\"false\",\n"
                        + "                        \"field\":\"snapshot\"\n"
                        + "                    },\n"
                        + "                    {\n"
                        + "                        \"type\":\"string\",\n"
                        + "                        \"optional\":false,\n"
                        + "                        \"field\":\"db\"\n"
                        + "                    },\n"
                        + "                    {\n"
                        + "                        \"type\":\"string\",\n"
                        + "                        \"optional\":true,\n"
                        + "                        \"field\":\"sequence\"\n"
                        + "                    },\n"
                        + "                    {\n"
                        + "                        \"type\":\"string\",\n"
                        + "                        \"optional\":true,\n"
                        + "                        \"field\":\"table\"\n"
                        + "                    },\n"
                        + "                    {\n"
                        + "                        \"type\":\"int64\",\n"
                        + "                        \"optional\":false,\n"
                        + "                        \"field\":\"server_id\"\n"
                        + "                    },\n"
                        + "                    {\n"
                        + "                        \"type\":\"string\",\n"
                        + "                        \"optional\":true,\n"
                        + "                        \"field\":\"gtid\"\n"
                        + "                    },\n"
                        + "                    {\n"
                        + "                        \"type\":\"string\",\n"
                        + "                        \"optional\":false,\n"
                        + "                        \"field\":\"file\"\n"
                        + "                    },\n"
                        + "                    {\n"
                        + "                        \"type\":\"int64\",\n"
                        + "                        \"optional\":false,\n"
                        + "                        \"field\":\"pos\"\n"
                        + "                    },\n"
                        + "                    {\n"
                        + "                        \"type\":\"int32\",\n"
                        + "                        \"optional\":false,\n"
                        + "                        \"field\":\"row\"\n"
                        + "                    },\n"
                        + "                    {\n"
                        + "                        \"type\":\"int64\",\n"
                        + "                        \"optional\":true,\n"
                        + "                        \"field\":\"thread\"\n"
                        + "                    },\n"
                        + "                    {\n"
                        + "                        \"type\":\"string\",\n"
                        + "                        \"optional\":true,\n"
                        + "                        \"field\":\"query\"\n"
                        + "                    }\n"
                        + "                ],\n"
                        + "                \"optional\":false,\n"
                        + "                \"name\":\"io.debezium.connector.mysql.Source\",\n"
                        + "                \"field\":\"source\"\n"
                        + "            },\n"
                        + "            {\n"
                        + "                \"type\":\"string\",\n"
                        + "                \"optional\":true,\n"
                        + "                \"field\":\"historyRecord\"\n"
                        + "            }\n"
                        + "        ],\n"
                        + "        \"optional\":false,\n"
                        + "        \"name\":\"io.debezium.connector.mysql.SchemaChangeValue\"\n"
                        + "    },\n"
                        + "    \"payload\":{\n"
                        + "        \"source\":{\n"
                        + "            \"version\":\"1.6.4.Final\",\n"
                        + "            \"connector\":\"mysql\",\n"
                        + "            \"name\":\"mysql_binlog_source\",\n"
                        + "            \"ts_ms\":1695203563233,\n"
                        + "            \"snapshot\":\"false\",\n"
                        + "            \"db\":\"tinyint1_not_bool_test\",\n"
                        + "            \"sequence\":null,\n"
                        + "            \"table\":\"t1\",\n"
                        + "            \"server_id\":223344,\n"
                        + "            \"gtid\":null,\n"
                        + "            \"file\":\"mysql-bin.000003\",\n"
                        + "            \"pos\":219,\n"
                        + "            \"row\":0,\n"
                        + "            \"thread\":null,\n"
                        + "            \"query\":null\n"
                        + "        },\n"
                        + "        \"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"mysql-bin.000003\\\",\\\"pos\\\":219,\\\"server_id\\\":223344},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1695203563,\\\"file\\\":\\\"mysql-bin.000003\\\",\\\"pos\\\":379,\\\"server_id\\\":223344},\\\"databaseName\\\":\\\"tinyint1_not_bool_test\\\",\\\"ddl\\\":\\\"ALTER TABLE t1 ADD COLUMN _new_tinyint1 TINYINT(1)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"tinyint1_not_bool_test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"latin1\\\",\\\"primaryKeyColumnNames\\\":[\\\"pk\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"pk\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"length\\\":11,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"_tinyint1\\\",\\\"jdbcType\\\":5,\\\"typeName\\\":\\\"TINYINT\\\",\\\"typeExpression\\\":\\\"TINYINT\\\",\\\"charsetName\\\":null,\\\"length\\\":1,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"_new_tinyint1\\\",\\\"jdbcType\\\":5,\\\"typeName\\\":\\\"TINYINT\\\",\\\"typeExpression\\\":\\\"TINYINT\\\",\\\"charsetName\\\":null,\\\"length\\\":1,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"\n"
                        + "    }\n"
                        + "}";
        DebeziumEvent debeziumEvent = objectMapper.readValue(json, DebeziumEvent.class);
        assertThat(debeziumEvent).isNotNull();
        assertThat(debeziumEvent.payload().hasHistoryRecord()).isTrue();
        Iterator<TableChanges.TableChange> tableChanges = debeziumEvent.payload().getTableChanges();
        assertThat(Iterators.size(tableChanges)).isEqualTo(1);
        tableChanges.forEachRemaining(
                tableChange -> {
                    assertThat(tableChange.getType()).isEqualTo(TableChanges.TableChangeType.ALTER);
                    assertThat(tableChange.getTable().id().toString())
                            .isEqualTo("tinyint1_not_bool_test.t1");
                });
    }
}
