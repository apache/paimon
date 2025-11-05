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

package org.apache.paimon.flink.pipeline.cdc.util;

import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.paimon.types.DataTypes.BIGINT;
import static org.apache.paimon.types.DataTypes.BINARY;
import static org.apache.paimon.types.DataTypes.BOOLEAN;
import static org.apache.paimon.types.DataTypes.BYTES;
import static org.apache.paimon.types.DataTypes.CHAR;
import static org.apache.paimon.types.DataTypes.DATE;
import static org.apache.paimon.types.DataTypes.DECIMAL;
import static org.apache.paimon.types.DataTypes.DOUBLE;
import static org.apache.paimon.types.DataTypes.FLOAT;
import static org.apache.paimon.types.DataTypes.INT;
import static org.apache.paimon.types.DataTypes.SMALLINT;
import static org.apache.paimon.types.DataTypes.STRING;
import static org.apache.paimon.types.DataTypes.TIME;
import static org.apache.paimon.types.DataTypes.TIMESTAMP;
import static org.apache.paimon.types.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.paimon.types.DataTypes.TINYINT;
import static org.apache.paimon.types.DataTypes.VARBINARY;
import static org.apache.paimon.types.DataTypes.VARCHAR;

/**
 * Type converter test for {@link FlinkCDCToPaimonTypeConverter} and {@link
 * PaimonToFlinkCDCTypeConverter}.
 */
public class TypeConverterTest {

    @Test
    public void testFullTypesConverter() {
        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn("pk_string", DataTypes.STRING().notNull())
                        .physicalColumn("boolean", DataTypes.BOOLEAN())
                        .physicalColumn("binary", DataTypes.BINARY(3))
                        .physicalColumn("varbinary", DataTypes.VARBINARY(10))
                        .physicalColumn("bytes", DataTypes.BYTES())
                        .physicalColumn("tinyint", DataTypes.TINYINT())
                        .physicalColumn("smallint", DataTypes.SMALLINT())
                        .physicalColumn("int", DataTypes.INT())
                        .physicalColumn("bigint", DataTypes.BIGINT())
                        .physicalColumn("float", DataTypes.FLOAT())
                        .physicalColumn("double", DataTypes.DOUBLE())
                        .physicalColumn("decimal", DataTypes.DECIMAL(6, 3))
                        .physicalColumn("char", DataTypes.CHAR(5))
                        .physicalColumn("varchar", DataTypes.VARCHAR(10))
                        .physicalColumn("string", DataTypes.STRING())
                        .physicalColumn("date", DataTypes.DATE())
                        .physicalColumn("time", DataTypes.TIME())
                        .physicalColumn("time_with_precision", DataTypes.TIME(6))
                        .physicalColumn("timestamp", DataTypes.TIMESTAMP())
                        .physicalColumn("timestamp_with_precision_3", DataTypes.TIMESTAMP(3))
                        .physicalColumn("timestamp_with_precision_6", DataTypes.TIMESTAMP(6))
                        .physicalColumn("timestamp_with_precision_9", DataTypes.TIMESTAMP(9))
                        .physicalColumn("timestamp_ltz", DataTypes.TIMESTAMP_LTZ())
                        .physicalColumn(
                                "timestamp_ltz_with_precision_3", DataTypes.TIMESTAMP_LTZ(3))
                        .physicalColumn(
                                "timestamp_ltz_with_precision_6", DataTypes.TIMESTAMP_LTZ(6))
                        .physicalColumn(
                                "timestamp_ltz_with_precision_9", DataTypes.TIMESTAMP_LTZ(9))
                        .primaryKey("pk_string")
                        .partitionKey("boolean")
                        .build();

        // Step 1: Convert CDC Schema to Paimon Schema
        org.apache.paimon.schema.Schema paimonSchema =
                FlinkCDCToPaimonTypeConverter.convertFlinkCDCSchemaToPaimonSchema(cdcSchema);

        // Step 2: Validate Paimon Schema
        Assertions.assertThat(paimonSchema)
                .isEqualTo(
                        org.apache.paimon.schema.Schema.newBuilder()
                                .primaryKey("pk_string")
                                .partitionKeys("boolean")
                                .column("pk_string", STRING().notNull())
                                .column("boolean", BOOLEAN())
                                .column("binary", BINARY(3))
                                .column("varbinary", VARBINARY(10))
                                .column("bytes", BYTES())
                                .column("tinyint", TINYINT())
                                .column("smallint", SMALLINT())
                                .column("int", INT())
                                .column("bigint", BIGINT())
                                .column("float", FLOAT())
                                .column("double", DOUBLE())
                                .column("decimal", DECIMAL(6, 3))
                                .column("char", CHAR(5))
                                .column("varchar", VARCHAR(10))
                                .column("string", STRING())
                                .column("date", DATE())
                                .column("time", TIME(0))
                                .column("time_with_precision", TIME(6))
                                .column("timestamp", TIMESTAMP(6))
                                .column("timestamp_with_precision_3", TIMESTAMP(3))
                                .column("timestamp_with_precision_6", TIMESTAMP(6))
                                .column("timestamp_with_precision_9", TIMESTAMP(9))
                                .column("timestamp_ltz", TIMESTAMP_WITH_LOCAL_TIME_ZONE(6))
                                .column(
                                        "timestamp_ltz_with_precision_3",
                                        TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                                .column(
                                        "timestamp_ltz_with_precision_6",
                                        TIMESTAMP_WITH_LOCAL_TIME_ZONE(6))
                                .column(
                                        "timestamp_ltz_with_precision_9",
                                        TIMESTAMP_WITH_LOCAL_TIME_ZONE(9))
                                .build());

        // Step 3: Convert it back
        Schema recreatedCdcSchema =
                PaimonToFlinkCDCTypeConverter.convertPaimonSchemaToFlinkCDCSchema(paimonSchema);
        Assertions.assertThat(recreatedCdcSchema).isEqualTo(cdcSchema);
    }
}
