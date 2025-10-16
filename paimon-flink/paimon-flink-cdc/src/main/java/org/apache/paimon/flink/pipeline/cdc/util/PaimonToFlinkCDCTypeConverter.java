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
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataType;

import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;

/** Type converter from Paimon to Flink CDC. */
public class PaimonToFlinkCDCTypeConverter {

    /** Convert Paimon schema to Flink CDC schema. */
    public static org.apache.flink.cdc.common.schema.Schema convertPaimonSchemaToFlinkCDCSchema(
            TableSchema schema) {
        if (schema == null) {
            return null;
        }

        return convertPaimonSchemaToFlinkCDCSchema(schema.toSchema());
    }

    /** Convert Paimon schema to Flink CDC schema. */
    public static org.apache.flink.cdc.common.schema.Schema convertPaimonSchemaToFlinkCDCSchema(
            Schema schema) {
        if (schema == null) {
            return null;
        }

        org.apache.flink.cdc.common.schema.Schema.Builder builder =
                new org.apache.flink.cdc.common.schema.Schema.Builder();
        schema.fields()
                .forEach(
                        (column) ->
                                builder.physicalColumn(
                                        column.name(),
                                        convertFlinkCDCDataTypeToPaimonDataType(column.type()),
                                        column.description(),
                                        column.defaultValue()));
        builder.primaryKey(schema.primaryKeys())
                .partitionKey(schema.partitionKeys())
                .comment(schema.comment())
                .options(schema.options());
        return builder.build();
    }

    /** Convert Paimon data type to Flink CDC data type. */
    public static org.apache.flink.cdc.common.types.DataType
            convertFlinkCDCDataTypeToPaimonDataType(DataType dataType) {
        return DataTypeUtils.fromFlinkDataType(
                TypeConversions.fromLogicalToDataType(
                        LogicalTypeConversion.toLogicalType(dataType)));
    }
}
