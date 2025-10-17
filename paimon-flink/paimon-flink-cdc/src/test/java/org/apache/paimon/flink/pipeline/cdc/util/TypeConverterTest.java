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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Type converter test for {@link FlinkCDCToPaimonTypeConverter} and {@link
 * PaimonToFlinkCDCTypeConverter}.
 */
public class TypeConverterTest {

    @Test
    public void testFullTypesConverter() {
        org.apache.flink.cdc.common.schema.Schema fullTypesSchema =
                org.apache.flink.cdc.common.schema.Schema.newBuilder()
                        .physicalColumn(
                                "pk_string",
                                org.apache.flink.cdc.common.types.DataTypes.STRING().notNull())
                        .physicalColumn(
                                "boolean", org.apache.flink.cdc.common.types.DataTypes.BOOLEAN())
                        .physicalColumn(
                                "binary", org.apache.flink.cdc.common.types.DataTypes.BINARY(3))
                        .physicalColumn(
                                "varbinary",
                                org.apache.flink.cdc.common.types.DataTypes.VARBINARY(10))
                        .physicalColumn(
                                "bytes", org.apache.flink.cdc.common.types.DataTypes.BYTES())
                        .physicalColumn(
                                "tinyint", org.apache.flink.cdc.common.types.DataTypes.TINYINT())
                        .physicalColumn(
                                "smallint", org.apache.flink.cdc.common.types.DataTypes.SMALLINT())
                        .physicalColumn("int", org.apache.flink.cdc.common.types.DataTypes.INT())
                        .physicalColumn(
                                "bigint", org.apache.flink.cdc.common.types.DataTypes.BIGINT())
                        .physicalColumn(
                                "float", org.apache.flink.cdc.common.types.DataTypes.FLOAT())
                        .physicalColumn(
                                "double", org.apache.flink.cdc.common.types.DataTypes.DOUBLE())
                        .physicalColumn(
                                "decimal",
                                org.apache.flink.cdc.common.types.DataTypes.DECIMAL(6, 3))
                        .physicalColumn("char", org.apache.flink.cdc.common.types.DataTypes.CHAR(5))
                        .physicalColumn(
                                "varchar", org.apache.flink.cdc.common.types.DataTypes.VARCHAR(10))
                        .physicalColumn(
                                "string", org.apache.flink.cdc.common.types.DataTypes.STRING())
                        .physicalColumn("date", org.apache.flink.cdc.common.types.DataTypes.DATE())
                        .physicalColumn("time", org.apache.flink.cdc.common.types.DataTypes.TIME())
                        .physicalColumn(
                                "time_with_precision",
                                org.apache.flink.cdc.common.types.DataTypes.TIME(6))
                        .physicalColumn(
                                "timestamp",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP())
                        .physicalColumn(
                                "timestamp_with_precision_3",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(3))
                        .physicalColumn(
                                "timestamp_with_precision_6",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(6))
                        .physicalColumn(
                                "timestamp_with_precision_9",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(9))
                        .physicalColumn(
                                "timestamp_ltz",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ())
                        .physicalColumn(
                                "timestamp_ltz_with_precision_3",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ(3))
                        .physicalColumn(
                                "timestamp_ltz_with_precision_6",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ(6))
                        .physicalColumn(
                                "timestamp_ltz_with_precision_9",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ(9))
                        .primaryKey("pk_string")
                        .partitionKey("boolean")
                        .build();

        Assertions.assertEquals(
                fullTypesSchema,
                PaimonToFlinkCDCTypeConverter.convertPaimonSchemaToFlinkCDCSchema(
                        FlinkCDCToPaimonTypeConverter.convertFlinkCDCSchemaToPaimonSchema(
                                fullTypesSchema)));
    }
}
