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
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.kafka.canal.CanalJsonEventParser;
import org.apache.paimon.flink.action.cdc.mysql.Expression;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.sink.cdc.CdcRecordUtils.toGenericRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link CanalJsonEventParser}. */
public class KafkaEventParserTest {
    private static final String CANAL_JSON_EVENT =
            "{\"data\":[{\"pt\":\"1\",\"_ID\":\"1\",\"v1\":\"one\","
                    + "\"_geometrycollection\":\"\\u0000\\u0000\\u0000\\u0000\\u0001\\u0007\\u0000\\u0000\\u0000\\u0003"
                    + "\\u0000\\u0000\\u0000\\u0001\\u0001\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000"
                    + "\\u0000$@\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000$@\\u0001\\u0001\\u0000\\u0000\\u0000\\u0000"
                    + "\\u0000\\u0000\\u0000\\u0000\\u0000>@\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000>@\\u0001\\u0002"
                    + "\\u0000\\u0000\\u0000\\u0002\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000"
                    + ".@\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000"
                    + ".@\\u0000\\u0000\\u0000\\u0000\\u0000\\u00004@\\u0000\\u0000\\u0000\\u0000\\u0000\\u00004@\","
                    + "\"_set\":\"3\",\"_enum\":\"1\"}],\"database\":\"paimon_sync_table\",\"es\":1683006706000,"
                    + "\"id\":92,\"isDdl\":false,\"mysqlType\":{\"pt\":\"INT\",\"_ID\":\"INT\",\"v1\":\"VARCHAR(10)\","
                    + "\"_geometrycollection\":\"GEOMETRYCOLLECTION\",\"_set\":\"SET('a','b','c','d')\",\"_enum\":\"ENUM"
                    + "('value1','value2','value3')\"},\"old\":null,\"pkNames\":[\"_id\"],\"sql\":\"\","
                    + "\"sqlType\":{\"pt\":4,\"_id\":4,\"v1\":12},\"table\":\"schema_evolution_1\",\"ts\":1683006706728,"
                    + "\"type\":\"INSERT\"}\n"
                    + ":null,\"database\":\"paimon_sync_table\",\"es\":1683168078000,\"id\":40,\"isDdl\":true,"
                    + "\"mysqlType\":null,\"old\":null,\"pkNames\":null,\"sql\":\"/* Query from "
                    + "DMS-WEBSQL-0-Qid_18293315143325386Y by user 1486767996652600 */ ALTER TABLE schema_evolution_1 "
                    + "MODIFY COLUMN v2 BIGINT\",\"sqlType\":null,\"table\":\"schema_evolution_1\",\"ts\":1683168078956,"
                    + "\"type\":\"ALTER\"}";
    private static final String CANAL_JSON_DDL_ADD_EVENT =
            "{\"data\":null,\"database\":\"paimon_sync_table\","
                    + "\"es\":1683008289000,\"id\":13,\"isDdl\":true,\"mysqlType\":null,\"old\":null,\"pkNames\":null,"
                    + "\"sql\":\" ALTER TABLE "
                    + "schema_evolution_1 ADD COLUMN v2 INT\",\"sqlType\":null,\"table\":\"schema_evolution_1\","
                    + "\"ts\":1683008289401,\"type\":\"ALTER\"}";

    private static final String CANAL_JSON_DDL_MODIFY_EVENT =
            "{\"data\":null,\"database\":\"paimon_sync_table\",\"es\":1683168155000,\"id\":54,\"isDdl\":true,"
                    + "\"mysqlType\":null,\"old\":null,\"pkNames\":null,\"sql\":\" ALTER TABLE schema_evolution_1 MODIFY "
                    + "COLUMN v1 VARCHAR(20)\",\"sqlType\":null,\"table\":\"schema_evolution_1\",\"ts\":1683168154943,"
                    + "\"type\":\"ALTER\"}";

    private static final String CANAL_JSON_DDL_DROP_EVENT =
            "{\"data\":null,\"database\":\"paimon_sync_table\",\"es\":1683168155000,\"id\":54,\"isDdl\":true,"
                    + "\"mysqlType\":null,\"old\":null,\"pkNames\":null,\"sql\":\" ALTER TABLE schema_evolution_1 DROP "
                    + "COLUMN v1 \",\"sqlType\":null,\"table\":\"schema_evolution_1\",\"ts\":1683168154943,"
                    + "\"type\":\"ALTER\"}";

    private static final String CANAL_JSON_DDL_CHANGE_EVENT =
            "{\"data\":null,\"database\":\"paimon_sync_table\",\"es\":1683168155000,\"id\":54,\"isDdl\":true,"
                    + "\"mysqlType\":null,\"old\":null,\"pkNames\":null,\"sql\":\" ALTER TABLE schema_evolution_1 CHANGE "
                    + "COLUMN `$% ^,& *(` cg VARCHAR(20) \",\"sqlType\":null,\"table\":\"schema_evolution_1\",\"ts\":1683168154943,"
                    + "\"type\":\"ALTER\"}";

    private static final String CANAL_JSON_DDL_MULTI_ADD_EVENT =
            "{\"data\":null,\"database\":\"paimon_sync_table\","
                    + "\"es\":1683614798000,\"id\":2431,\"isDdl\":true,\"mysqlType\":null,\"old\":null,\"pkNames\":null,"
                    + "\"sql\":\" ALTER TABLE "
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
        boolean caseSensitive = false;
        EventParser<String> parser =
                new CanalJsonEventParser(caseSensitive, new TableNameConverter(caseSensitive));
        parser.setRawEvent(CANAL_JSON_EVENT);
        List<DataField> dataFields = new ArrayList<>();
        dataFields.add(new DataField(0, "pt", DataTypes.INT()));
        dataFields.add(new DataField(1, "_id", DataTypes.INT()));
        dataFields.add(new DataField(2, "v1", DataTypes.VARCHAR(10)));
        dataFields.add(new DataField(3, "_geometrycollection", DataTypes.STRING()));
        dataFields.add(new DataField(4, "_set", DataTypes.ARRAY(DataTypes.STRING())));
        dataFields.add(new DataField(5, "_enum", DataTypes.STRING()));
        assertThat(parser.parseSchemaChange()).isEmpty();
        List<GenericRow> result =
                parser.parseRecords().stream()
                        .map(record -> toGenericRow(record, dataFields).get())
                        .collect(Collectors.toList());
        BinaryString[] binaryStrings =
                new BinaryString[] {BinaryString.fromString("a"), BinaryString.fromString("b")};
        GenericArray genericArray = new GenericArray(binaryStrings);

        List<GenericRow> expect =
                Collections.singletonList(
                        GenericRow.of(
                                1,
                                1,
                                BinaryString.fromString("one"),
                                BinaryString.fromString(
                                        "{\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0}"),
                                genericArray,
                                BinaryString.fromString("value1")));
        assertThat(result).isEqualTo(expect);
    }

    @Test
    public void testCanalJsonEventParserDdl() {
        boolean caseSensitive = false;
        EventParser<String> parser =
                new CanalJsonEventParser(caseSensitive, new TableNameConverter(caseSensitive));
        parser.setRawEvent(CANAL_JSON_EVENT);
        List<DataField> expectDataFields = new ArrayList<>();
        expectDataFields.add(new DataField(0, "v2", DataTypes.INT()));
        assertThat(parser.parseSchemaChange()).isEmpty();
        parser.setRawEvent(CANAL_JSON_DDL_ADD_EVENT);
        List<DataField> updatedDataFieldsAdd = parser.parseSchemaChange();
        assertThat(updatedDataFieldsAdd).isEqualTo(expectDataFields);

        expectDataFields.clear();
        expectDataFields.add(new DataField(0, "v1", DataTypes.VARCHAR(20)));
        parser.setRawEvent(CANAL_JSON_DDL_MODIFY_EVENT);
        List<DataField> updatedDataFieldsModify = parser.parseSchemaChange();
        assertThat(updatedDataFieldsModify).isEqualTo(expectDataFields);
        expectDataFields.clear();

        expectDataFields.add(new DataField(0, "v4", DataTypes.INT()));
        expectDataFields.add(new DataField(1, "v1", DataTypes.VARCHAR(30)));

        expectDataFields.add(new DataField(2, "v5", DataTypes.DOUBLE()));
        expectDataFields.add(new DataField(3, "v6", DataTypes.DECIMAL(5, 3)));
        expectDataFields.add(new DataField(4, "$% ^,& *(", DataTypes.VARCHAR(10)));
        expectDataFields.add(new DataField(5, "v2", DataTypes.BIGINT()));

        parser.setRawEvent(CANAL_JSON_DDL_MULTI_ADD_EVENT);
        List<DataField> updatedDataFieldsMulti = parser.parseSchemaChange();
        assertThat(updatedDataFieldsMulti).isEqualTo(expectDataFields);
        expectDataFields.clear();
        parser.setRawEvent(CANAL_JSON_DDL_CHANGE_EVENT);
        List<DataField> updatedDataFieldsChange = parser.parseSchemaChange();
        expectDataFields.add(new DataField(0, "cg", DataTypes.VARCHAR(20)));
        assertThat(updatedDataFieldsChange).isEqualTo(expectDataFields);
    }

    @Test
    public void testCanalJsonEventParserParseDebeziumJson() {
        boolean caseSensitive = true;
        EventParser<String> parser =
                new CanalJsonEventParser(caseSensitive, new TableNameConverter(caseSensitive));
        parser.setRawEvent(DEBEZIUM_JSON_EVENT);
        RuntimeException e =
                assertThrows(
                        RuntimeException.class, parser::parseRecords, "Expecting RuntimeException");
        assertThat(e)
                .hasMessage(
                        "CanalJsonEventParser only supports canal-json format,please make sure that your topic's format is accurate.");
    }

    @Test
    public void testCaseSensitive() {
        EventParser<String> parserCaseInsensitive =
                new CanalJsonEventParser(false, new TableNameConverter(false));
        EventParser<String> parserCaseSensitive =
                new CanalJsonEventParser(true, new TableNameConverter(true));
        parserCaseInsensitive.setRawEvent(CANAL_JSON_EVENT);
        List<DataField> dataFields = new ArrayList<>();
        dataFields.add(new DataField(0, "pt", DataTypes.INT()));
        dataFields.add(new DataField(1, "_id", DataTypes.INT()));
        dataFields.add(new DataField(2, "v1", DataTypes.VARCHAR(10)));
        dataFields.add(new DataField(3, "_geometrycollection", DataTypes.STRING()));
        dataFields.add(new DataField(4, "_set", DataTypes.ARRAY(DataTypes.STRING())));
        dataFields.add(new DataField(5, "_enum", DataTypes.STRING()));
        assertThat(parserCaseInsensitive.parseSchemaChange()).isEmpty();
        List<GenericRow> result =
                parserCaseInsensitive.parseRecords().stream()
                        .map(record -> toGenericRow(record, dataFields).get())
                        .collect(Collectors.toList());
        BinaryString[] binaryStrings =
                new BinaryString[] {BinaryString.fromString("a"), BinaryString.fromString("b")};
        GenericArray genericArray = new GenericArray(binaryStrings);

        List<GenericRow> expect =
                Collections.singletonList(
                        GenericRow.of(
                                1,
                                1,
                                BinaryString.fromString("one"),
                                BinaryString.fromString(
                                        "{\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0}"),
                                genericArray,
                                BinaryString.fromString("value1")));
        assertThat(result).isEqualTo(expect);
        parserCaseSensitive.setRawEvent(CANAL_JSON_EVENT);
        Optional<GenericRow> genericRow =
                toGenericRow(parserCaseSensitive.parseRecords().get(0), dataFields);
        assert !genericRow.isPresent();
        dataFields.remove(1);
        dataFields.add(1, new DataField(1, "_ID", DataTypes.INT()));
        List<GenericRow> resultCaseSensitive =
                parserCaseSensitive.parseRecords().stream()
                        .map(record -> toGenericRow(record, dataFields).get())
                        .collect(Collectors.toList());
        assertThat(resultCaseSensitive).isEqualTo(expect);
    }

    @Test
    public void testCanalJsonEventParserAndComputeColumn() {
        boolean caseSensitive = false;
        EventParser<String> parser =
                new CanalJsonEventParser(
                        caseSensitive,
                        new TableNameConverter(caseSensitive),
                        Collections.singletonList(
                                new ComputedColumn("v1", Expression.substring("v1", "1"))));
        parser.setRawEvent(CANAL_JSON_EVENT);
        List<DataField> dataFields = new ArrayList<>();
        dataFields.add(new DataField(0, "pt", DataTypes.INT()));
        dataFields.add(new DataField(1, "_id", DataTypes.INT()));
        dataFields.add(new DataField(2, "v1", DataTypes.VARCHAR(10)));
        dataFields.add(new DataField(3, "_geometrycollection", DataTypes.STRING()));
        dataFields.add(new DataField(4, "_set", DataTypes.ARRAY(DataTypes.STRING())));
        dataFields.add(new DataField(5, "_enum", DataTypes.STRING()));
        assertThat(parser.parseSchemaChange()).isEmpty();
        List<GenericRow> result =
                parser.parseRecords().stream()
                        .map(record -> toGenericRow(record, dataFields).get())
                        .collect(Collectors.toList());
        BinaryString[] binaryStrings =
                new BinaryString[] {BinaryString.fromString("a"), BinaryString.fromString("b")};
        GenericArray genericArray = new GenericArray(binaryStrings);

        List<GenericRow> expect =
                Collections.singletonList(
                        GenericRow.of(
                                1,
                                1,
                                BinaryString.fromString("ne"),
                                BinaryString.fromString(
                                        "{\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0}"),
                                genericArray,
                                BinaryString.fromString("value1")));
        assertThat(result).isEqualTo(expect);
    }
}
