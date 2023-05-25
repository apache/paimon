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

package org.apache.paimon.flink.action.cdc.kafka;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.kafka.canal.CanalJsonEventParser;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link CanalJsonEventParser}. */
public class KafkaEventParserTest {
    private static final String CANAL_JSON_EVENT =
            "{\"data\":[{\"pt\":\"1\",\"_id\":\"1\",\"v1\":\"one\"}],"
                    + "\"database\":\"paimon_sync_table\",\"es\":1683006706000,\"id\":92,\"isDdl\":false,"
                    + "\"mysqlType\":{\"pt\":\"INT\",\"_id\":\"INT\",\"v1\":\"VARCHAR(10)\"},\"old\":null,\"pkNames\":[\"_id\"],"
                    + "\"sql\":\"\",\"sqlType\":{\"pt\":4,\"_id\":4,\"v1\":12},\"table\":\"schema_evolution_1\","
                    + "\"ts\":1683006706728,\"type\":\"INSERT\"}\n";
    private static final String CANAL_JSON_DDL_ADD_EVENT =
            "{\"data\":null,\"database\":\"paimon_sync_table\","
                    + "\"es\":1683008289000,\"id\":13,\"isDdl\":true,\"mysqlType\":null,\"old\":null,\"pkNames\":null,"
                    + "\"sql\":\"/* Query from DMS-WEBSQL-0-Qid_1694572663426906u by user 1486767996652600 */ ALTER TABLE "
                    + "schema_evolution_1 ADD COLUMN v2 INT\",\"sqlType\":null,\"table\":\"schema_evolution_1\","
                    + "\"ts\":1683008289401,\"type\":\"ALTER\"}";
    private static final String CANAL_JSON_DDL_MODIFY_EVENT =
            "{\"data\":null,\"database\":\"paimon_sync_table\",\"es\":1683168155000,\"id\":54,\"isDdl\":true,"
                    + "\"mysqlType\":null,\"old\":null,\"pkNames\":null,\"sql\":\"/* Query from "
                    + "DMS-WEBSQL-0-Qid_29438367264981907i by user 1486767996652600 */ ALTER TABLE schema_evolution_1 MODIFY "
                    + "COLUMN v1 VARCHAR(20)\",\"sqlType\":null,\"table\":\"schema_evolution_1\",\"ts\":1683168154943,"
                    + "\"type\":\"ALTER\"}";

    private static final String CANAL_JSON_DDL_MultiAdd_EVENT =
            "{\"data\":null,\"database\":\"paimon_sync_table\","
                    + "\"es\":1683614798000,\"id\":2431,\"isDdl\":true,\"mysqlType\":null,\"old\":null,\"pkNames\":null,"
                    + "\"sql\":\"/* Query from DMS-WEBSQL-0-Qid_29885012146961120X by user 1486767996652600 */ ALTER TABLE "
                    + "schema_evolution_multiple \\nADD v4 INT,\\nMODIFY COLUMN v1 VARCHAR(30),\\nADD COLUMN (v5 DOUBLE, v6 "
                    + "DECIMAL(5, 3), `$% ^,& *(` VARCHAR(10) COMMENT 'test'),\\n            MODIFY v2 BIGINT\",\"sqlType\":null,"
                    + "\"table\":\"schema_evolution_multiple\",\"ts\":1683614799031,\"type\":\"ALTER\"}\n";

    private static final String DEBEZIUM_JSON_EVENT =
            "{\"before\":null,\"after\":{\"id\":101,\"name\":\"scooter\","
                    + "\"description\":\"Small 2-wheel scooter\",\"weight\":3.140000104904175},\"source\":{\"version\":\"1.1.1"
                    + ".Final\",\"connector\":\"mysql\",\"name\":\"dbserver1\",\"ts_ms\":0,\"snapshot\":\"true\","
                    + "\"db\":\"inventory\",\"table\":\"products\",\"server_id\":0,\"gtid\":null,\"file\":\"mysql-bin.000003\","
                    + "\"pos\":154,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"c\",\"ts_ms\":1589355606100,"
                    + "\"transaction\":null}\n";

    @Test
    public void testCanalJsonEventParser() {
        boolean caseSensitive = true;
        EventParser<String> parser =
                new CanalJsonEventParser(caseSensitive, new TableNameConverter(caseSensitive));
        parser.setRawEvent(CANAL_JSON_EVENT);
        List<DataField> dataFields = new ArrayList<>();
        dataFields.add(new DataField(0, "pt", DataTypes.INT()));
        dataFields.add(new DataField(1, "_id", DataTypes.INT()));
        dataFields.add(new DataField(2, "v1", DataTypes.VARCHAR(10)));
        List<DataField> updatedDataFields = parser.getUpdatedDataFields().orElse(null);
        assert updatedDataFields == null;
        List<GenericRow> result =
                parser.getRecords().stream()
                        .map(record -> record.toGenericRow(dataFields).get())
                        .collect(Collectors.toList());
        List<GenericRow> expect =
                Collections.singletonList(GenericRow.of(1, 1, BinaryString.fromString("one")));
        assertThat(result).isEqualTo(expect);
    }

    @Test
    public void testCanalJsonEventParserDdl() {
        boolean caseSensitive = true;
        EventParser<String> parser =
                new CanalJsonEventParser(caseSensitive, new TableNameConverter(caseSensitive));
        parser.setRawEvent(CANAL_JSON_EVENT);
        List<DataField> expectDataFields = new ArrayList<>();
        expectDataFields.add(new DataField(0, "pt", DataTypes.INT()));
        expectDataFields.add(new DataField(1, "_id", DataTypes.INT()));
        expectDataFields.add(new DataField(2, "v1", DataTypes.VARCHAR(10)));
        expectDataFields.add(new DataField(3, "v2", DataTypes.INT()));
        List<DataField> updatedDataFields = parser.getUpdatedDataFields().orElse(null);
        assert updatedDataFields == null;
        parser.setRawEvent(CANAL_JSON_DDL_ADD_EVENT);
        List<DataField> updatedDataFieldsAdd = parser.getUpdatedDataFields().orElse(null);
        assertThat(updatedDataFieldsAdd).isEqualTo(expectDataFields);

        expectDataFields.remove(2);
        expectDataFields.add(new DataField(4, "v1", DataTypes.VARCHAR(20)));
        parser.setRawEvent(CANAL_JSON_DDL_MODIFY_EVENT);
        List<DataField> updatedDataFieldsModify = parser.getUpdatedDataFields().orElse(null);
        assertThat(updatedDataFieldsModify).isEqualTo(expectDataFields);
        expectDataFields.remove(2);
        expectDataFields.remove(2);
        expectDataFields.add(new DataField(4, "v4", DataTypes.INT()));
        expectDataFields.add(new DataField(4, "v1", DataTypes.VARCHAR(30)));
        expectDataFields.add(new DataField(4, "v5", DataTypes.DOUBLE()));
        expectDataFields.add(new DataField(4, "v6", DataTypes.DECIMAL(5, 3)));
        expectDataFields.add(new DataField(4, "$% ^,& *(", DataTypes.VARCHAR(10)));
        expectDataFields.add(new DataField(4, "v2", DataTypes.BIGINT()));
        parser.setRawEvent(CANAL_JSON_DDL_MultiAdd_EVENT);
        List<DataField> updatedDataFieldsMulti = parser.getUpdatedDataFields().orElse(null);
        assertThat(updatedDataFieldsMulti).isEqualTo(expectDataFields);
    }

    @Test
    public void testCanalJsonEventParserParseDebeziumJson() {
        boolean caseSensitive = true;
        EventParser<String> parser =
                new CanalJsonEventParser(caseSensitive, new TableNameConverter(caseSensitive));

        RuntimeException e =
                assertThrows(
                        RuntimeException.class,
                        () -> parser.setRawEvent(DEBEZIUM_JSON_EVENT),
                        "Expecting RuntimeException");
        assertThat(e)
                .hasMessage(
                        "java.lang.NullPointerException: CanalJsonEventParser only supports canal-json format,please make sure that your topic's format is accurate.");
    }
}
