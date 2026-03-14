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

import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataType;

import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;

/** Type converter from Flink CDC to Paimon. */
public class FlinkCDCToPaimonTypeConverter {

    public static final String DEFAULT_DATETIME = "1970-01-01 00:00:00";

    public static final String INVALID_OR_MISSING_DATETIME = "0000-00-00 00:00:00";

    /** Convert Flink CDC schema to Paimon schema. */
    public static Schema convertFlinkCDCSchemaToPaimonSchema(
            org.apache.flink.cdc.common.schema.Schema schema) {
        Schema.Builder builder = new Schema.Builder();
        schema.getColumns()
                .forEach(
                        (column) ->
                                builder.column(
                                        column.getName(),
                                        convertFlinkCDCDataTypeToPaimonDataType(column.getType()),
                                        column.getComment(),
                                        convertFlinkCDCDefaultValueToValidValue(
                                                column.getDefaultValueExpression(),
                                                column.getType())));
        builder.primaryKey(schema.primaryKeys())
                .partitionKeys(schema.partitionKeys())
                .comment(schema.comment())
                .options(schema.options());
        return builder.build();
    }

    /** Convert Flink CDC data type to Paimon data type. */
    public static DataType convertFlinkCDCDataTypeToPaimonDataType(
            org.apache.flink.cdc.common.types.DataType dataType) {
        return LogicalTypeConversion.toDataType(
                DataTypeUtils.toFlinkDataType(dataType).getLogicalType());
    }

    /** Convert Flink CDC default value to a valid value of Paimon. */
    public static String convertFlinkCDCDefaultValueToValidValue(
            String defaultValue, org.apache.flink.cdc.common.types.DataType dataType) {
        if (defaultValue == null) {
            return null;
        }

        if (dataType instanceof LocalZonedTimestampType
                || dataType instanceof TimestampType
                || dataType instanceof ZonedTimestampType) {

            if (INVALID_OR_MISSING_DATETIME.equals(defaultValue)) {
                return DEFAULT_DATETIME;
            }
        }

        return defaultValue;
    }
}
