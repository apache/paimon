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

package org.apache.paimon.flink.action.cdc.format.maxwell;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link MaxwellRecordParser}. */
public class MaxwellRecordParserTest {

    private static final String INSERT_EVENT =
            "{\"database\":\"test\",\"table\":\"product\",\"type\":\"insert\","
                    + "\"ts\":1739500001,\"xid\":7125,\"xoffset\":0,"
                    + "\"data\":{\"id\":101},"
                    + "\"primary_key_columns\":[\"id\"]}";

    /**
     * With {@code output_ddl} enabled, Maxwell writes DDL events to the data topic by default. They
     * have no 'data' field, so parsing them used to fail the sync job with "missing 'data' field"
     * on every restart. They must be skipped like the DDL records of the other formats.
     */
    @ParameterizedTest
    @CsvSource({
        "database-create, CREATE DATABASE test",
        "database-alter, ALTER DATABASE test CHARACTER SET utf8mb4",
        "database-drop, DROP DATABASE test",
        "table-create, CREATE TABLE product (id INT PRIMARY KEY)",
        "table-alter, ALTER TABLE product AUTO_INCREMENT = 2300",
        "table-drop, DROP TABLE product"
    })
    public void testSkipDdlEvents(String type, String sql) throws Exception {
        String ddlEvent =
                "{\"type\":\""
                        + type
                        + "\",\"database\":\"test\",\"table\":\"product\","
                        + "\"def\":{\"database\":\"test\",\"charset\":\"utf8mb4\","
                        + "\"table\":\"product\",\"primary-key\":[\"id\"],"
                        + "\"columns\":[{\"type\":\"int\",\"name\":\"id\",\"signed\":true}]},"
                        + "\"ts\":1739500000000,\"sql\":\""
                        + sql
                        + "\",\"position\":\"mysql-bin.000003:4815\"}";

        MaxwellRecordParser parser =
                new MaxwellRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        List<RichCdcMultiplexRecord> records = new ArrayList<>();

        // schema discovery of kafka_sync_table moves on to the next record
        assertThat(parser.buildSchema(toRecord(ddlEvent))).isNull();
        parser.flatMap(toRecord(ddlEvent), new ListCollector<>(records));
        assertThat(records).isEmpty();

        // the data change after the DDL event is still synchronized
        parser.flatMap(toRecord(INSERT_EVENT), new ListCollector<>(records));
        assertThat(records).hasSize(1);
        CdcRecord cdcRecord = records.get(0).toRichCdcRecord().toCdcRecord();
        assertThat(cdcRecord.kind()).isEqualTo(RowKind.INSERT);
        assertThat(cdcRecord.data()).containsEntry("id", "101");
    }

    private static CdcSourceRecord toRecord(String json) throws Exception {
        return new CdcSourceRecord(new ObjectMapper().readValue(json, JsonNode.class));
    }
}
