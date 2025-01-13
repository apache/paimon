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

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.Base64Variants;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.JsonNodeType;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.execution.JobClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

/** IT cases for {@link KafkaSyncTableAction}. */
public class KafkaDebeziumAvroSyncTableActionITCase extends KafkaActionITCaseBase {

    // Serializer for debezium avro format
    protected static final Schema NULL_AVRO_SCHEMA = Schema.create(Schema.Type.NULL);
    protected KafkaAvroSerializer kafkaKeyAvroSerializer;
    protected KafkaAvroSerializer kafkaValueAvroSerializer;

    private static final String ALL_TYPES_TABLE_KEY_SCHEMA =
            "{\"type\":\"record\",\"name\":\"Key\",\"namespace\":\"test_avro.workdb.all_types_table\",\"fields\":[{\"name\":\"_id\",\"type\":\"int\"}],\"connect.name\":\"test_avro.workdb.all_types_table.Key\"}";
    private static final String ALL_TYPES_TABLE_VALUE_SCHEMA =
            "{\"type\":\"record\",\"name\":\"Envelope\",\"namespace\":\"test_avro.workdb.all_types_table\",\"fields\":[{\"name\":\"before\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"_id\",\"type\":{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"_id\"}}},{\"name\":\"pt\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DECIMAL\",\"__debezium.source.column.length\":\"2\",\"__debezium.source.column.scale\":\"1\",\"__debezium.source.column.name\":\"pt\"}}],\"default\":null},{\"name\":\"_tinyint1\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TINYINT\",\"__debezium.source.column.length\":\"1\",\"__debezium.source.column.name\":\"_tinyint1\"},\"connect.type\":\"int16\"}],\"default\":null},{\"name\":\"_boolean\",\"type\":[\"null\",{\"type\":\"boolean\",\"connect.parameters\":{\"__debezium.source.column.type\":\"BOOLEAN\",\"__debezium.source.column.name\":\"_boolean\"}}],\"default\":null},{\"name\":\"_bool\",\"type\":[\"null\",{\"type\":\"boolean\",\"connect.parameters\":{\"__debezium.source.column.type\":\"BOOL\",\"__debezium.source.column.name\":\"_bool\"}}],\"default\":null},{\"name\":\"_tinyint\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TINYINT\",\"__debezium.source.column.name\":\"_tinyint\"},\"connect.type\":\"int16\"}],\"default\":null},{\"name\":\"_tinyint_unsigned\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TINYINT UNSIGNED\",\"__debezium.source.column.length\":\"2\",\"__debezium.source.column.name\":\"_tinyint_unsigned\"},\"connect.type\":\"int16\"}],\"default\":null},{\"name\":\"_tinyint_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TINYINT UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"2\",\"__debezium.source.column.name\":\"_tinyint_unsigned_zerofill\"},\"connect.type\":\"int16\"}],\"default\":null},{\"name\":\"_smallint\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"SMALLINT\",\"__debezium.source.column.name\":\"_smallint\"},\"connect.type\":\"int16\"}],\"default\":null},{\"name\":\"_smallint_unsigned\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"SMALLINT UNSIGNED\",\"__debezium.source.column.name\":\"_smallint_unsigned\"}}],\"default\":null},{\"name\":\"_smallint_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"SMALLINT UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"4\",\"__debezium.source.column.name\":\"_smallint_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_mediumint\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"MEDIUMINT\",\"__debezium.source.column.name\":\"_mediumint\"}}],\"default\":null},{\"name\":\"_mediumint_unsigned\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"MEDIUMINT UNSIGNED\",\"__debezium.source.column.name\":\"_mediumint_unsigned\"}}],\"default\":null},{\"name\":\"_mediumint_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"MEDIUMINT UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.name\":\"_mediumint_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_int\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"_int\"}}],\"default\":null},{\"name\":\"_int_unsigned\",\"type\":[\"null\",{\"type\":\"long\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT UNSIGNED\",\"__debezium.source.column.name\":\"_int_unsigned\"}}],\"default\":null},{\"name\":\"_int_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"long\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.name\":\"_int_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_bigint\",\"type\":[\"null\",{\"type\":\"long\",\"connect.parameters\":{\"__debezium.source.column.type\":\"BIGINT\",\"__debezium.source.column.name\":\"_bigint\"}}],\"default\":null},{\"name\":\"_bigint_unsigned\",\"type\":[\"null\",{\"type\":\"long\",\"connect.parameters\":{\"__debezium.source.column.type\":\"BIGINT UNSIGNED\",\"__debezium.source.column.name\":\"_bigint_unsigned\"}}],\"default\":null},{\"name\":\"_bigint_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"long\",\"connect.parameters\":{\"__debezium.source.column.type\":\"BIGINT UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"16\",\"__debezium.source.column.name\":\"_bigint_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_serial\",\"type\":{\"type\":\"long\",\"connect.parameters\":{\"__debezium.source.column.type\":\"BIGINT UNSIGNED\",\"__debezium.source.column.name\":\"_serial\"}}},{\"name\":\"_float\",\"type\":[\"null\",{\"type\":\"float\",\"connect.parameters\":{\"__debezium.source.column.type\":\"FLOAT\",\"__debezium.source.column.name\":\"_float\"}}],\"default\":null},{\"name\":\"_float_unsigned\",\"type\":[\"null\",{\"type\":\"float\",\"connect.parameters\":{\"__debezium.source.column.type\":\"FLOAT UNSIGNED\",\"__debezium.source.column.name\":\"_float_unsigned\"}}],\"default\":null},{\"name\":\"_float_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"float\",\"connect.parameters\":{\"__debezium.source.column.type\":\"FLOAT UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"4\",\"__debezium.source.column.name\":\"_float_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_real\",\"type\":[\"null\",{\"type\":\"float\",\"connect.parameters\":{\"__debezium.source.column.type\":\"REAL\",\"__debezium.source.column.name\":\"_real\"}}],\"default\":null},{\"name\":\"_real_unsigned\",\"type\":[\"null\",{\"type\":\"float\",\"connect.parameters\":{\"__debezium.source.column.type\":\"REAL UNSIGNED\",\"__debezium.source.column.name\":\"_real_unsigned\"}}],\"default\":null},{\"name\":\"_real_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"float\",\"connect.parameters\":{\"__debezium.source.column.type\":\"REAL UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"10\",\"__debezium.source.column.scale\":\"7\",\"__debezium.source.column.name\":\"_real_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_double\",\"type\":[\"null\",{\"type\":\"double\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DOUBLE\",\"__debezium.source.column.name\":\"_double\"}}],\"default\":null},{\"name\":\"_double_unsigned\",\"type\":[\"null\",{\"type\":\"double\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DOUBLE UNSIGNED\",\"__debezium.source.column.name\":\"_double_unsigned\"}}],\"default\":null},{\"name\":\"_double_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"double\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DOUBLE UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"10\",\"__debezium.source.column.scale\":\"7\",\"__debezium.source.column.name\":\"_double_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_double_precision\",\"type\":[\"null\",{\"type\":\"double\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DOUBLE PRECISION\",\"__debezium.source.column.name\":\"_double_precision\"}}],\"default\":null},{\"name\":\"_double_precision_unsigned\",\"type\":[\"null\",{\"type\":\"double\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DOUBLE PRECISION UNSIGNED\",\"__debezium.source.column.name\":\"_double_precision_unsigned\"}}],\"default\":null},{\"name\":\"_double_precision_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"double\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DOUBLE PRECISION UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"10\",\"__debezium.source.column.scale\":\"7\",\"__debezium.source.column.name\":\"_double_precision_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_numeric\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"NUMERIC\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"_numeric\"}}],\"default\":null},{\"name\":\"_numeric_unsigned\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"NUMERIC UNSIGNED\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"_numeric_unsigned\"}}],\"default\":null},{\"name\":\"_numeric_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"NUMERIC UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"_numeric_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_fixed\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"FIXED\",\"__debezium.source.column.length\":\"40\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"_fixed\"}}],\"default\":null},{\"name\":\"_fixed_unsigned\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"FIXED UNSIGNED\",\"__debezium.source.column.length\":\"40\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"_fixed_unsigned\"}}],\"default\":null},{\"name\":\"_fixed_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"FIXED UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"40\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"_fixed_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_decimal\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DECIMAL\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"0\",\"__debezium.source.column.name\":\"_decimal\"}}],\"default\":null},{\"name\":\"_decimal_unsigned\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DECIMAL UNSIGNED\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"0\",\"__debezium.source.column.name\":\"_decimal_unsigned\"}}],\"default\":null},{\"name\":\"_decimal_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DECIMAL UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"0\",\"__debezium.source.column.name\":\"_decimal_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_date\",\"type\":[\"null\",{\"type\":\"int\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"DATE\",\"__debezium.source.column.name\":\"_date\"},\"connect.name\":\"io.debezium.time.Date\"}],\"default\":null},{\"name\":\"_datetime\",\"type\":[\"null\",{\"type\":\"long\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"DATETIME\",\"__debezium.source.column.name\":\"_datetime\"},\"connect.name\":\"io.debezium.time.Timestamp\"}],\"default\":null},{\"name\":\"_datetime3\",\"type\":[\"null\",{\"type\":\"long\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"DATETIME\",\"__debezium.source.column.length\":\"3\",\"__debezium.source.column.name\":\"_datetime3\"},\"connect.name\":\"io.debezium.time.Timestamp\"}],\"default\":null},{\"name\":\"_datetime6\",\"type\":[\"null\",{\"type\":\"long\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"DATETIME\",\"__debezium.source.column.length\":\"6\",\"__debezium.source.column.name\":\"_datetime6\"},\"connect.name\":\"io.debezium.time.MicroTimestamp\"}],\"default\":null},{\"name\":\"_datetime_p\",\"type\":[\"null\",{\"type\":\"long\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"DATETIME\",\"__debezium.source.column.name\":\"_datetime_p\"},\"connect.name\":\"io.debezium.time.Timestamp\"}],\"default\":null},{\"name\":\"_datetime_p2\",\"type\":[\"null\",{\"type\":\"long\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"DATETIME\",\"__debezium.source.column.length\":\"2\",\"__debezium.source.column.name\":\"_datetime_p2\"},\"connect.name\":\"io.debezium.time.Timestamp\"}],\"default\":null},{\"name\":\"_timestamp\",\"type\":[\"null\",{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"TIMESTAMP\",\"__debezium.source.column.length\":\"6\",\"__debezium.source.column.name\":\"_timestamp\"},\"connect.name\":\"io.debezium.time.ZonedTimestamp\"}],\"default\":null},{\"name\":\"_timestamp0\",\"type\":[\"null\",{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"TIMESTAMP\",\"__debezium.source.column.name\":\"_timestamp0\"},\"connect.name\":\"io.debezium.time.ZonedTimestamp\"}],\"default\":null},{\"name\":\"_char\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"CHAR\",\"__debezium.source.column.length\":\"10\",\"__debezium.source.column.name\":\"_char\"}}],\"default\":null},{\"name\":\"_varchar\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"20\",\"__debezium.source.column.name\":\"_varchar\"}}],\"default\":null},{\"name\":\"_tinytext\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TINYTEXT\",\"__debezium.source.column.name\":\"_tinytext\"}}],\"default\":null},{\"name\":\"_text\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TEXT\",\"__debezium.source.column.name\":\"_text\"}}],\"default\":null},{\"name\":\"_mediumtext\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"MEDIUMTEXT\",\"__debezium.source.column.name\":\"_mediumtext\"}}],\"default\":null},{\"name\":\"_longtext\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"LONGTEXT\",\"__debezium.source.column.name\":\"_longtext\"}}],\"default\":null},{\"name\":\"_bin\",\"type\":[\"null\",{\"type\":\"bytes\",\"connect.parameters\":{\"__debezium.source.column.type\":\"BINARY\",\"__debezium.source.column.length\":\"10\",\"__debezium.source.column.name\":\"_bin\"}}],\"default\":null},{\"name\":\"_varbin\",\"type\":[\"null\",{\"type\":\"bytes\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARBINARY\",\"__debezium.source.column.length\":\"20\",\"__debezium.source.column.name\":\"_varbin\"}}],\"default\":null},{\"name\":\"_tinyblob\",\"type\":[\"null\",{\"type\":\"bytes\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TINYBLOB\",\"__debezium.source.column.name\":\"_tinyblob\"}}],\"default\":null},{\"name\":\"_blob\",\"type\":[\"null\",{\"type\":\"bytes\",\"connect.parameters\":{\"__debezium.source.column.type\":\"BLOB\",\"__debezium.source.column.name\":\"_blob\"}}],\"default\":null},{\"name\":\"_mediumblob\",\"type\":[\"null\",{\"type\":\"bytes\",\"connect.parameters\":{\"__debezium.source.column.type\":\"MEDIUMBLOB\",\"__debezium.source.column.name\":\"_mediumblob\"}}],\"default\":null},{\"name\":\"_longblob\",\"type\":[\"null\",{\"type\":\"bytes\",\"connect.parameters\":{\"__debezium.source.column.type\":\"LONGBLOB\",\"__debezium.source.column.name\":\"_longblob\"}}],\"default\":null},{\"name\":\"_json\",\"type\":[\"null\",{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"JSON\",\"__debezium.source.column.name\":\"_json\"},\"connect.name\":\"io.debezium.data.Json\"}],\"default\":null},{\"name\":\"_enum\",\"type\":[\"null\",{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"value1,value2,value3\",\"__debezium.source.column.type\":\"ENUM\",\"__debezium.source.column.length\":\"1\",\"__debezium.source.column.name\":\"_enum\"},\"connect.name\":\"io.debezium.data.Enum\"}],\"default\":null},{\"name\":\"_year\",\"type\":[\"null\",{\"type\":\"int\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"YEAR\",\"__debezium.source.column.name\":\"_year\"},\"connect.name\":\"io.debezium.time.Year\"}],\"default\":null},{\"name\":\"_time\",\"type\":[\"null\",{\"type\":\"long\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"TIME\",\"__debezium.source.column.name\":\"_time\"},\"connect.name\":\"io.debezium.time.MicroTime\"}],\"default\":null},{\"name\":\"_point\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Point\",\"namespace\":\"io.debezium.data.geometry\",\"fields\":[{\"name\":\"x\",\"type\":\"double\"},{\"name\":\"y\",\"type\":\"double\"},{\"name\":\"wkb\",\"type\":[\"null\",\"bytes\"],\"default\":null},{\"name\":\"srid\",\"type\":[\"null\",\"int\"],\"default\":null}],\"connect.doc\":\"Geometry (POINT)\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"POINT\",\"__debezium.source.column.name\":\"_point\"},\"connect.name\":\"io.debezium.data.geometry.Point\"}],\"default\":null},{\"name\":\"_geometry\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Geometry\",\"namespace\":\"io.debezium.data.geometry\",\"fields\":[{\"name\":\"wkb\",\"type\":\"bytes\"},{\"name\":\"srid\",\"type\":[\"null\",\"int\"],\"default\":null}],\"connect.doc\":\"Geometry\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"GEOMETRY\",\"__debezium.source.column.name\":\"_geometry\"},\"connect.name\":\"io.debezium.data.geometry.Geometry\"}],\"default\":null},{\"name\":\"_linestring\",\"type\":[\"null\",\"io.debezium.data.geometry.Geometry\"],\"default\":null},{\"name\":\"_polygon\",\"type\":[\"null\",\"io.debezium.data.geometry.Geometry\"],\"default\":null},{\"name\":\"_multipoint\",\"type\":[\"null\",\"io.debezium.data.geometry.Geometry\"],\"default\":null},{\"name\":\"_multiline\",\"type\":[\"null\",\"io.debezium.data.geometry.Geometry\"],\"default\":null},{\"name\":\"_multipolygon\",\"type\":[\"null\",\"io.debezium.data.geometry.Geometry\"],\"default\":null},{\"name\":\"_geometrycollection\",\"type\":[\"null\",\"io.debezium.data.geometry.Geometry\"],\"default\":null},{\"name\":\"_set\",\"type\":[\"null\",{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"a,b,c,d\",\"__debezium.source.column.type\":\"SET\",\"__debezium.source.column.length\":\"7\",\"__debezium.source.column.name\":\"_set\"},\"connect.name\":\"io.debezium.data.EnumSet\"}],\"default\":null}],\"connect.name\":\"test_avro.workdb.all_types_table.Value\"}],\"default\":null},{\"name\":\"after\",\"type\":[\"null\",\"Value\"],\"default\":null},{\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"Source\",\"namespace\":\"io.debezium.connector.mysql\",\"fields\":[{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"connector\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":\"long\"},{\"name\":\"snapshot\",\"type\":[{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"true,last,false,incremental\"},\"connect.default\":\"false\",\"connect.name\":\"io.debezium.data.Enum\"},\"null\"],\"default\":\"false\"},{\"name\":\"db\",\"type\":\"string\"},{\"name\":\"sequence\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"table\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"server_id\",\"type\":\"long\"},{\"name\":\"gtid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file\",\"type\":\"string\"},{\"name\":\"pos\",\"type\":\"long\"},{\"name\":\"row\",\"type\":\"int\"},{\"name\":\"thread\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"query\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.name\":\"io.debezium.connector.mysql.Source\"}},{\"name\":\"op\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"transaction\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"block\",\"namespace\":\"event\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"total_order\",\"type\":\"long\"},{\"name\":\"data_collection_order\",\"type\":\"long\"}],\"connect.version\":1,\"connect.name\":\"event.block\"}],\"default\":null}],\"connect.version\":1,\"connect.name\":\"test_avro.workdb.all_types_table.Envelope\"}";
    private Schema allTypesTableKeySchema;
    private Schema allTypesTableValueSchema;
    private Schema debeziumSourceSchema;

    @BeforeEach
    public void setup() {
        // Init avro serializer for kafka key/value
        Map<String, Object> props = new HashMap<>();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrl());
        props.put(AUTO_REGISTER_SCHEMAS, true);
        kafkaKeyAvroSerializer = new KafkaAvroSerializer();
        kafkaKeyAvroSerializer.configure(props, true);
        kafkaValueAvroSerializer = new KafkaAvroSerializer();
        kafkaValueAvroSerializer.configure(props, false);

        // Init kafka key/value schema
        Schema.Parser parser = new Schema.Parser();
        allTypesTableKeySchema = parser.parse(ALL_TYPES_TABLE_KEY_SCHEMA);
        allTypesTableValueSchema = parser.parse(ALL_TYPES_TABLE_VALUE_SCHEMA);
        debeziumSourceSchema = allTypesTableValueSchema.getField("source").schema();
    }

    @Test
    @Timeout(60)
    public void testAllTypes() throws Exception {
        // the first round checks for table creation
        // the second round checks for running the action on an existing table
        for (int i = 0; i < 2; i++) {
            testAllTypesOnce();
            Thread.sleep(3000);
        }
    }

    private void testAllTypesOnce() throws Exception {
        final String topic = "all_type" + UUID.randomUUID();
        createTestTopic(topic, 1, 1);
        writeRecordsToKafka(
                topic, "kafka/debezium-avro/table/schema/alltype/debezium-avro-data-1.txt");

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "debezium-avro");
        kafkaConfig.put("topic", topic);
        kafkaConfig.put("schema.registry.url", getSchemaRegistryUrl());

        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pt", "_id")
                        .build();
        JobClient client = runActionWithDefaultEnv(action);

        testAllTypesImpl();
        client.cancel().get();
    }

    protected void writeRecordsToKafka(String topic, String resourceDirFormat, Object... args)
            throws Exception {
        Properties producerProperties = getStandardProps();
        producerProperties.setProperty("retries", "0");
        producerProperties.put("key.serializer", ByteArraySerializer.class.getName());
        producerProperties.put("value.serializer", ByteArraySerializer.class.getName());

        URL url =
                KafkaActionITCaseBase.class
                        .getClassLoader()
                        .getResource(String.format(resourceDirFormat, args));
        List<String> lines =
                Files.readAllLines(Paths.get(url.toURI())).stream()
                        .filter(this::isRecordLine)
                        .collect(Collectors.toList());
        try (KafkaProducer<byte[], byte[]> kafkaProducer =
                new KafkaProducer<>(producerProperties)) {
            for (int i = 0; i < lines.size(); i += 2) {
                JsonNode key = objectMapper.readTree(lines.get(i));
                JsonNode value = objectMapper.readTree(lines.get(i + 1));
                JsonNode keyPayload = key.get("payload");
                JsonNode valuePayload = value.get("payload");
                JsonNode source = valuePayload.get("source");
                JsonNode after = valuePayload.get("after");

                GenericRecord avroKey = new GenericData.Record(allTypesTableKeySchema);
                avroKey.put("_id", keyPayload.get("_id").asInt());

                GenericRecord avroValue = new GenericData.Record(allTypesTableValueSchema);
                Schema beforeSchema = allTypesTableValueSchema.getField("before").schema();
                GenericRecord afterAvroValue =
                        new GenericData.Record(sanitizedSchema(beforeSchema));
                afterAvroValue.put("_id", after.get("_id").asInt());
                afterAvroValue.put("pt", after.get("pt").asText());
                if (nonNullNode(after.get("_tinyint1"))) {
                    afterAvroValue.put("_tinyint1", after.get("_tinyint1").asInt());
                }
                if (nonNullNode(after.get("_boolean"))) {
                    afterAvroValue.put("_boolean", after.get("_boolean").asBoolean());
                }
                if (nonNullNode(after.get("_bool"))) {
                    afterAvroValue.put("_bool", after.get("_bool").asBoolean());
                }
                if (nonNullNode(after.get("_tinyint"))) {
                    afterAvroValue.put("_tinyint", after.get("_tinyint").asInt());
                }
                if (nonNullNode(after.get("_tinyint_unsigned"))) {
                    afterAvroValue.put("_tinyint_unsigned", after.get("_tinyint_unsigned").asInt());
                }
                if (nonNullNode(after.get("_tinyint_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_tinyint_unsigned_zerofill",
                            after.get("_tinyint_unsigned_zerofill").asInt());
                }
                if (nonNullNode(after.get("_smallint"))) {
                    afterAvroValue.put("_smallint", after.get("_smallint").asInt());
                }
                if (nonNullNode(after.get("_smallint_unsigned"))) {
                    afterAvroValue.put(
                            "_smallint_unsigned", after.get("_smallint_unsigned").asInt());
                }
                if (nonNullNode(after.get("_smallint_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_smallint_unsigned_zerofill",
                            after.get("_smallint_unsigned_zerofill").asInt());
                }
                if (nonNullNode(after.get("_mediumint"))) {
                    afterAvroValue.put("_mediumint", after.get("_mediumint").asInt());
                }
                if (nonNullNode(after.get("_mediumint_unsigned"))) {
                    afterAvroValue.put(
                            "_mediumint_unsigned", after.get("_mediumint_unsigned").asInt());
                }
                if (nonNullNode(after.get("_mediumint_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_mediumint_unsigned_zerofill",
                            after.get("_mediumint_unsigned_zerofill").asInt());
                }
                if (nonNullNode(after.get("_int"))) {
                    afterAvroValue.put("_int", after.get("_int").asInt());
                }
                if (nonNullNode(after.get("_int_unsigned"))) {
                    afterAvroValue.put("_int_unsigned", after.get("_int_unsigned").asLong());
                }
                if (nonNullNode(after.get("_int_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_int_unsigned_zerofill", after.get("_int_unsigned_zerofill").asLong());
                }
                if (nonNullNode(after.get("_bigint"))) {
                    afterAvroValue.put("_bigint", after.get("_bigint").asLong());
                }
                if (nonNullNode(after.get("_bigint_unsigned"))) {
                    afterAvroValue.put("_bigint_unsigned", after.get("_bigint_unsigned").asLong());
                }
                if (nonNullNode(after.get("_bigint_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_bigint_unsigned_zerofill",
                            after.get("_bigint_unsigned_zerofill").asLong());
                }
                afterAvroValue.put("_serial", after.get("_serial").asLong());
                if (nonNullNode(after.get("_float"))) {
                    afterAvroValue.put("_float", after.get("_float").floatValue());
                }
                if (nonNullNode(after.get("_float_unsigned"))) {
                    afterAvroValue.put(
                            "_float_unsigned", after.get("_float_unsigned").floatValue());
                }
                if (nonNullNode(after.get("_float_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_float_unsigned_zerofill",
                            after.get("_float_unsigned_zerofill").floatValue());
                }
                if (nonNullNode(after.get("_real"))) {
                    afterAvroValue.put("_real", after.get("_real").floatValue());
                }
                if (nonNullNode(after.get("_real_unsigned"))) {
                    afterAvroValue.put("_real_unsigned", after.get("_real_unsigned").floatValue());
                }
                if (nonNullNode(after.get("_real_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_real_unsigned_zerofill",
                            after.get("_real_unsigned_zerofill").floatValue());
                }
                if (nonNullNode(after.get("_double"))) {
                    afterAvroValue.put("_double", after.get("_double").asDouble());
                }
                if (nonNullNode(after.get("_double_unsigned"))) {
                    afterAvroValue.put(
                            "_double_unsigned", after.get("_double_unsigned").asDouble());
                }
                if (nonNullNode(after.get("_double_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_double_unsigned_zerofill",
                            after.get("_double_unsigned_zerofill").asDouble());
                }
                if (nonNullNode(after.get("_double_precision"))) {
                    afterAvroValue.put(
                            "_double_precision", after.get("_double_precision").asDouble());
                }
                if (nonNullNode(after.get("_double_precision_unsigned"))) {
                    afterAvroValue.put(
                            "_double_precision_unsigned",
                            after.get("_double_precision_unsigned").asDouble());
                }
                if (nonNullNode(after.get("_double_precision_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_double_precision_unsigned_zerofill",
                            after.get("_double_precision_unsigned_zerofill").asDouble());
                }
                // Decimal types
                if (nonNullNode(after.get("_numeric"))) {
                    afterAvroValue.put("_numeric", after.get("_numeric").asText());
                }
                if (nonNullNode(after.get("_numeric_unsigned"))) {
                    afterAvroValue.put(
                            "_numeric_unsigned", after.get("_numeric_unsigned").asText());
                }
                if (nonNullNode(after.get("_numeric_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_numeric_unsigned_zerofill",
                            after.get("_numeric_unsigned_zerofill").asText());
                }
                if (nonNullNode(after.get("_fixed"))) {
                    afterAvroValue.put("_fixed", after.get("_fixed").asText());
                }
                if (nonNullNode(after.get("_fixed_unsigned"))) {
                    afterAvroValue.put("_fixed_unsigned", after.get("_fixed_unsigned").asText());
                }
                if (nonNullNode(after.get("_fixed_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_fixed_unsigned_zerofill",
                            after.get("_fixed_unsigned_zerofill").asText());
                }
                if (nonNullNode(after.get("_decimal"))) {
                    afterAvroValue.put("_decimal", after.get("_decimal").asText());
                }
                if (nonNullNode(after.get("_decimal_unsigned"))) {
                    afterAvroValue.put(
                            "_decimal_unsigned", after.get("_decimal_unsigned").asText());
                }
                if (nonNullNode(after.get("_decimal_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_decimal_unsigned_zerofill",
                            after.get("_decimal_unsigned_zerofill").asText());
                }
                // Date types
                if (nonNullNode(after.get("_date"))) {
                    afterAvroValue.put("_date", after.get("_date").asInt());
                }
                if (nonNullNode(after.get("_datetime"))) {
                    afterAvroValue.put("_datetime", after.get("_datetime").asLong());
                }
                if (nonNullNode(after.get("_datetime3"))) {
                    afterAvroValue.put("_datetime3", after.get("_datetime3").asLong());
                }
                if (nonNullNode(after.get("_datetime6"))) {
                    afterAvroValue.put("_datetime6", after.get("_datetime6").asLong());
                }
                if (nonNullNode(after.get("_datetime_p"))) {
                    afterAvroValue.put("_datetime_p", after.get("_datetime_p").asLong());
                }
                if (nonNullNode(after.get("_datetime_p2"))) {
                    afterAvroValue.put("_datetime_p2", after.get("_datetime_p2").asLong());
                }
                if (nonNullNode(after.get("_timestamp"))) {
                    afterAvroValue.put("_timestamp", after.get("_timestamp").asText());
                }
                if (nonNullNode(after.get("_timestamp0"))) {
                    afterAvroValue.put("_timestamp0", after.get("_timestamp0").asText());
                }
                // String types
                if (nonNullNode(after.get("_char"))) {
                    afterAvroValue.put("_char", after.get("_char").asText());
                }
                if (nonNullNode(after.get("_varchar"))) {
                    afterAvroValue.put("_varchar", after.get("_varchar").asText());
                }
                if (nonNullNode(after.get("_tinytext"))) {
                    afterAvroValue.put("_tinytext", after.get("_tinytext").asText());
                }
                if (nonNullNode(after.get("_text"))) {
                    afterAvroValue.put("_text", after.get("_text").asText());
                }
                if (nonNullNode(after.get("_mediumtext"))) {
                    afterAvroValue.put("_mediumtext", after.get("_mediumtext").asText());
                }
                if (nonNullNode(after.get("_longtext"))) {
                    afterAvroValue.put("_longtext", after.get("_longtext").asText());
                }
                // Bytes
                if (nonNullNode(after.get("_bin"))) {
                    afterAvroValue.put(
                            "_bin",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_bin").asText())));
                }
                if (nonNullNode(after.get("_varbin"))) {
                    afterAvroValue.put(
                            "_varbin",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_varbin").asText())));
                }
                if (nonNullNode(after.get("_tinyblob"))) {
                    afterAvroValue.put(
                            "_tinyblob",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_tinyblob").asText())));
                }
                if (nonNullNode(after.get("_blob"))) {
                    afterAvroValue.put(
                            "_blob",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_blob").asText())));
                }
                if (nonNullNode(after.get("_mediumblob"))) {
                    afterAvroValue.put(
                            "_mediumblob",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_mediumblob").asText())));
                }
                if (nonNullNode(after.get("_longblob"))) {
                    afterAvroValue.put(
                            "_longblob",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_longblob").asText())));
                }
                // Json
                if (nonNullNode(after.get("_json"))) {
                    afterAvroValue.put("_json", after.get("_json").asText());
                }
                // Enum
                if (nonNullNode(after.get("_enum"))) {
                    afterAvroValue.put("_enum", after.get("_enum").asText());
                }

                if (nonNullNode(after.get("_year"))) {
                    afterAvroValue.put("_year", after.get("_year").asInt());
                }
                if (nonNullNode(after.get("_time"))) {
                    afterAvroValue.put("_time", after.get("_time").asLong());
                }
                // Point
                JsonNode pointJsonValue = after.get("_point");
                if (nonNullNode(pointJsonValue)) {
                    Schema pointSchema =
                            sanitizedSchema(
                                    sanitizedSchema(beforeSchema).getField("_point").schema());
                    afterAvroValue.put("_point", buildPointRecord(pointJsonValue, pointSchema));
                }
                // Geometry
                JsonNode geometryJsonValue = after.get("_geometry");
                Schema geometrySchema =
                        sanitizedSchema(
                                sanitizedSchema(beforeSchema).getField("_geometry").schema());
                if (nonNullNode(geometryJsonValue)) {
                    afterAvroValue.put(
                            "_geometry", buildGeometryRecord(geometryJsonValue, geometrySchema));
                }

                JsonNode linestringJsonNode = after.get("_linestring");
                if (nonNullNode(linestringJsonNode)) {
                    afterAvroValue.put(
                            "_linestring", buildGeometryRecord(linestringJsonNode, geometrySchema));
                }

                JsonNode polygonJsonNode = after.get("_polygon");
                if (nonNullNode(polygonJsonNode)) {
                    afterAvroValue.put(
                            "_polygon", buildGeometryRecord(polygonJsonNode, geometrySchema));
                }

                JsonNode multipointJsonNode = after.get("_multipoint");
                if (nonNullNode(multipointJsonNode)) {
                    afterAvroValue.put(
                            "_multipoint", buildGeometryRecord(multipointJsonNode, geometrySchema));
                }

                JsonNode multilineJsonNode = after.get("_multiline");
                if (nonNullNode(multilineJsonNode)) {
                    afterAvroValue.put(
                            "_multiline", buildGeometryRecord(multilineJsonNode, geometrySchema));
                }

                JsonNode multipolygonJsonNode = after.get("_multipolygon");
                if (nonNullNode(multipolygonJsonNode)) {
                    afterAvroValue.put(
                            "_multipolygon",
                            buildGeometryRecord(multipolygonJsonNode, geometrySchema));
                }

                JsonNode geometrycollectionJsonNode = after.get("_geometrycollection");
                if (nonNullNode(geometrycollectionJsonNode)) {
                    afterAvroValue.put(
                            "_geometrycollection",
                            buildGeometryRecord(geometrycollectionJsonNode, geometrySchema));
                }
                // Set
                if (nonNullNode(after.get("_set"))) {
                    afterAvroValue.put("_set", after.get("_set").asText());
                }

                avroValue.put("after", afterAvroValue);
                // Common properties
                avroValue.put("source", buildDebeziumSourceProperty(debeziumSourceSchema, source));
                avroValue.put("op", valuePayload.get("op").asText());
                avroValue.put("ts_ms", valuePayload.get("ts_ms").asLong());

                // Write to kafka
                kafkaProducer.send(
                        new ProducerRecord<>(
                                topic,
                                kafkaKeyAvroSerializer.serialize(topic, avroKey),
                                kafkaValueAvroSerializer.serialize(topic, avroValue)));
            }
        }
    }

    private boolean nonNullNode(JsonNode jsonNode) {
        return jsonNode != null && jsonNode.getNodeType() != JsonNodeType.NULL;
    }

    private GenericRecord buildPointRecord(JsonNode pointJsonValue, Schema debeziumPointSchema) {
        GenericRecord pointAvroValue = new GenericData.Record(debeziumPointSchema);
        pointAvroValue.put("x", pointJsonValue.get("x").asDouble());
        pointAvroValue.put("y", pointJsonValue.get("y").asDouble());
        pointAvroValue.put(
                "wkb",
                ByteBuffer.wrap(
                        Base64Variants.getDefaultVariant()
                                .decode(pointJsonValue.get("wkb").asText())));
        pointAvroValue.put(
                "srid",
                pointJsonValue.get("srid") != null ? pointJsonValue.get("srid").asInt() : null);
        return pointAvroValue;
    }

    private GenericRecord buildGeometryRecord(
            JsonNode geometryJsonValue, Schema debeziumGeometrySchema) {
        GenericRecord geometryAvroValue = new GenericData.Record(debeziumGeometrySchema);
        geometryAvroValue.put(
                "wkb",
                ByteBuffer.wrap(
                        Base64Variants.getDefaultVariant()
                                .decode(geometryJsonValue.get("wkb").asText())));
        geometryAvroValue.put(
                "srid",
                geometryJsonValue.get("srid") != null
                        ? geometryJsonValue.get("srid").asInt()
                        : null);
        return geometryAvroValue;
    }

    /** For all types test case. */
    protected void testAllTypesImpl() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), // _id
                            DataTypes.DECIMAL(2, 1).notNull(), // pt
                            DataTypes.BOOLEAN(), // _tinyint1
                            DataTypes.BOOLEAN(), // _boolean
                            DataTypes.BOOLEAN(), // _bool
                            DataTypes.TINYINT(), // _tinyint
                            DataTypes.SMALLINT(), // _tinyint_unsigned
                            DataTypes.SMALLINT(), // _tinyint_unsigned_zerofill
                            DataTypes.SMALLINT(), // _smallint
                            DataTypes.INT(), // _smallint_unsigned
                            DataTypes.INT(), // _smallint_unsigned_zerofill
                            DataTypes.INT(), // _mediumint
                            DataTypes.BIGINT(), // _mediumint_unsigned
                            DataTypes.BIGINT(), // _mediumint_unsigned_zerofill
                            DataTypes.INT(), // _int
                            DataTypes.BIGINT(), // _int_unsigned
                            DataTypes.BIGINT(), // _int_unsigned_zerofill
                            DataTypes.BIGINT(), // _bigint
                            DataTypes.DECIMAL(20, 0), // _bigint_unsigned
                            DataTypes.DECIMAL(20, 0), // _bigint_unsigned_zerofill
                            DataTypes.DECIMAL(20, 0), // _serial
                            DataTypes.FLOAT(), // _float
                            DataTypes.FLOAT(), // _float_unsigned
                            DataTypes.FLOAT(), // _float_unsigned_zerofill
                            DataTypes.DOUBLE(), // _real
                            DataTypes.DOUBLE(), // _real_unsigned
                            DataTypes.DOUBLE(), // _real_unsigned_zerofill
                            DataTypes.DOUBLE(), // _double
                            DataTypes.DOUBLE(), // _double_unsigned
                            DataTypes.DOUBLE(), // _double_unsigned_zerofill
                            DataTypes.DOUBLE(), // _double_precision
                            DataTypes.DOUBLE(), // _double_precision_unsigned
                            DataTypes.DOUBLE(), // _double_precision_unsigned_zerofill
                            DataTypes.DECIMAL(8, 3), // _numeric
                            DataTypes.DECIMAL(8, 3), // _numeric_unsigned
                            DataTypes.DECIMAL(8, 3), // _numeric_unsigned_zerofill
                            DataTypes.STRING(), // _fixed
                            DataTypes.STRING(), // _fixed_unsigned
                            DataTypes.STRING(), // _fixed_unsigned_zerofill
                            DataTypes.DECIMAL(8, 0), // _decimal
                            DataTypes.DECIMAL(8, 0), // _decimal_unsigned
                            DataTypes.DECIMAL(8, 0), // _decimal_unsigned_zerofill
                            DataTypes.DATE(), // _date
                            DataTypes.TIMESTAMP(0), // _datetime
                            DataTypes.TIMESTAMP(3), // _datetime3
                            DataTypes.TIMESTAMP(6), // _datetime6
                            DataTypes.TIMESTAMP(0), // _datetime_p
                            DataTypes.TIMESTAMP(2), // _datetime_p2
                            DataTypes.TIMESTAMP(6), // _timestamp
                            DataTypes.TIMESTAMP(0), // _timestamp0
                            DataTypes.CHAR(10), // _char
                            DataTypes.VARCHAR(20), // _varchar
                            DataTypes.STRING(), // _tinytext
                            DataTypes.STRING(), // _text
                            DataTypes.STRING(), // _mediumtext
                            DataTypes.STRING(), // _longtext
                            DataTypes.VARBINARY(10), // _bin
                            DataTypes.VARBINARY(20), // _varbin
                            DataTypes.BYTES(), // _tinyblob
                            DataTypes.BYTES(), // _blob
                            DataTypes.BYTES(), // _mediumblob
                            DataTypes.BYTES(), // _longblob
                            DataTypes.STRING(), // _json
                            DataTypes.STRING(), // _enum
                            DataTypes.INT(), // _year
                            DataTypes.TIME(), // _time
                            DataTypes.STRING(), // _point
                            DataTypes.STRING(), // _geometry
                            DataTypes.STRING(), // _linestring
                            DataTypes.STRING(), // _polygon
                            DataTypes.STRING(), // _multipoint
                            DataTypes.STRING(), // _multiline
                            DataTypes.STRING(), // _multipolygon
                            DataTypes.STRING(), // _geometrycollection
                            DataTypes.ARRAY(DataTypes.STRING()) // _set
                        },
                        new String[] {
                            "_id",
                            "pt",
                            "_tinyint1",
                            "_boolean",
                            "_bool",
                            "_tinyint",
                            "_tinyint_unsigned",
                            "_tinyint_unsigned_zerofill",
                            "_smallint",
                            "_smallint_unsigned",
                            "_smallint_unsigned_zerofill",
                            "_mediumint",
                            "_mediumint_unsigned",
                            "_mediumint_unsigned_zerofill",
                            "_int",
                            "_int_unsigned",
                            "_int_unsigned_zerofill",
                            "_bigint",
                            "_bigint_unsigned",
                            "_bigint_unsigned_zerofill",
                            "_serial",
                            "_float",
                            "_float_unsigned",
                            "_float_unsigned_zerofill",
                            "_real",
                            "_real_unsigned",
                            "_real_unsigned_zerofill",
                            "_double",
                            "_double_unsigned",
                            "_double_unsigned_zerofill",
                            "_double_precision",
                            "_double_precision_unsigned",
                            "_double_precision_unsigned_zerofill",
                            "_numeric",
                            "_numeric_unsigned",
                            "_numeric_unsigned_zerofill",
                            "_fixed",
                            "_fixed_unsigned",
                            "_fixed_unsigned_zerofill",
                            "_decimal",
                            "_decimal_unsigned",
                            "_decimal_unsigned_zerofill",
                            "_date",
                            "_datetime",
                            "_datetime3",
                            "_datetime6",
                            "_datetime_p",
                            "_datetime_p2",
                            "_timestamp",
                            "_timestamp0",
                            "_char",
                            "_varchar",
                            "_tinytext",
                            "_text",
                            "_mediumtext",
                            "_longtext",
                            "_bin",
                            "_varbin",
                            "_tinyblob",
                            "_blob",
                            "_mediumblob",
                            "_longblob",
                            "_json",
                            "_enum",
                            "_year",
                            "_time",
                            "_point",
                            "_geometry",
                            "_linestring",
                            "_polygon",
                            "_multipoint",
                            "_multiline",
                            "_multipolygon",
                            "_geometrycollection",
                            "_set",
                        });
        FileStoreTable table = getFileStoreTable(tableName);
        List<String> expected =
                Arrays.asList(
                        "+I["
                                + "1, 1.1, "
                                + "true, true, false, 1, 2, 3, "
                                + "1000, 2000, 3000, "
                                + "100000, 200000, 300000, "
                                + "1000000, 2000000, 3000000, "
                                + "10000000000, 20000000000, 30000000000, 40000000000, "
                                + "1.5, 2.5, 3.5, "
                                + "1.000001, 2.000002, 3.000003, "
                                + "1.000011, 2.000022, 3.000033, "
                                + "1.000111, 2.000222, 3.000333, "
                                + "12345.110, 12345.220, 12345.330, "
                                + "123456789876543212345678987654321.110, 123456789876543212345678987654321.220, 123456789876543212345678987654321.330, "
                                + "11111, 22222, 33333, "
                                + "19439, "
                                // display value of datetime is not affected by timezone
                                + "2023-03-23T14:30:05, 2023-03-23T14:30:05.123, 2023-03-23T14:30:05.123456, "
                                + "2023-03-24T14:30, 2023-03-24T14:30:05.120, "
                                // TODO haven't handle zone
                                + "2023-03-23T07:00:10.123456, 2023-03-22T16:10, "
                                + "Paimon, Apache Paimon, Apache Paimon MySQL TINYTEXT Test Data, Apache Paimon MySQL Test Data, Apache Paimon MySQL MEDIUMTEXT Test Data, Apache Paimon MySQL Long Test Data, "
                                + "[98, 121, 116, 101, 115, 0, 0, 0, 0, 0], "
                                + "[109, 111, 114, 101, 32, 98, 121, 116, 101, 115], "
                                + "[84, 73, 78, 89, 66, 76, 79, 66, 32, 116, 121, 112, 101, 32, 116, 101, 115, 116, 32, 100, 97, 116, 97], "
                                + "[66, 76, 79, 66, 32, 116, 121, 112, 101, 32, 116, 101, 115, 116, 32, 100, 97, 116, 97], "
                                + "[77, 69, 68, 73, 85, 77, 66, 76, 79, 66, 32, 116, 121, 112, 101, 32, 116, 101, 115, 116, 32, 100, 97, 116, 97], "
                                + "[76, 79, 78, 71, 66, 76, 79, 66, 32, 32, 98, 121, 116, 101, 115, 32, 116, 101, 115, 116, 32, 100, 97, 116, 97], "
                                + "{\"a\": \"b\"}, "
                                + "value1, "
                                + "2023, "
                                + "36803000, "
                                + "{\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}, "
                                + "{\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}, "
                                + "{\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0}, "
                                + "{\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}, "
                                + "{\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0}, "
                                + "{\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0}, "
                                + "{\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0}, "
                                + "{\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0}, "
                                + "[a, b]"
                                + "]",
                        "+I["
                                + "2, 2.2, "
                                + "NULL, NULL, NULL, NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, 50000000000, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, "
                                + "NULL, NULL, "
                                + "NULL, NULL, NULL, NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, NULL, NULL, NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL"
                                + "]");

        waitForResult(expected, table, rowType, Arrays.asList("pt", "_id"));
    }

    private Schema sanitizedSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION
                && schema.getTypes().size() == 2
                && schema.getTypes().contains(NULL_AVRO_SCHEMA)) {
            for (Schema memberSchema : schema.getTypes()) {
                if (!memberSchema.equals(NULL_AVRO_SCHEMA)) {
                    return memberSchema;
                }
            }
        }
        return schema;
    }

    private GenericRecord buildDebeziumSourceProperty(Schema sourceSchema, JsonNode sourceValue) {
        GenericRecord source = new GenericData.Record(sourceSchema);
        source.put("version", sourceValue.get("version").asText());
        source.put("connector", sourceValue.get("connector").asText());
        source.put("name", sourceValue.get("name").asText());
        source.put("ts_ms", sourceValue.get("ts_ms").asLong());
        source.put("snapshot", sourceValue.get("snapshot").asText());
        source.put("db", sourceValue.get("db").asText());
        source.put(
                "sequence",
                sourceValue.get("sequence") == null ? null : sourceValue.get("sequence").asText());
        source.put("table", sourceValue.get("table").asText());
        source.put("server_id", sourceValue.get("server_id").asLong());
        source.put(
                "gtid", sourceValue.get("gtid") == null ? null : sourceValue.get("gtid").asText());
        source.put("file", sourceValue.get("file").asText());
        source.put("pos", sourceValue.get("pos").asLong());
        source.put("row", sourceValue.get("row").asInt());
        source.put("thread", sourceValue.get("thread").asLong());
        source.put(
                "query",
                sourceValue.get("query") == null ? null : sourceValue.get("query").asText());
        return source;
    }
}
