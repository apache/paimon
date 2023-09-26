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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.relational.history.TableChanges;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DebeziumEvent}. */
public class DebeziumEventTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    public void before() {
        objectMapper = new ObjectMapper();
        objectMapper
                .configure(JsonParser.Feature.ALLOW_COMMENTS, true)
                .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Test
    public void testDeserialize() throws IOException {
        final URL url =
                DebeziumEventTest.class
                        .getClassLoader()
                        .getResource("mysql/debezium-event-change.json");
        assertThat(url).isNotNull();
        DebeziumEvent debeziumEvent = objectMapper.readValue(url, DebeziumEvent.class);
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
