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

package org.apache.paimon.flink.action.cdc.mysql.format;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.json.JsonConverterConfig;

import javax.annotation.Nullable;

import java.math.BigDecimal;

/** Converts from Debezium type to {@link DataType}. */
public class DebeziumTypeUtils {

    // Debezium data types
    // https://debezium.io/documentation/reference/3.1/connectors/mysql.html#mysql-data-types
    private static final String BOOLEAN = "BOOLEAN";
    private static final String BYTES = "BYTES";
    private static final String INT16 = "INT16";
    private static final String INT32 = "INT32";
    private static final String INT64 = "INT64";
    private static final String FLOAT32 = "FLOAT32";
    private static final String FLOAT64 = "FLOAT64";
    private static final String STRING = "STRING";
    private static final String DOUBLE = "DOUBLE";

    public static DataType toDataType(String type, @Nullable String className, String rawValue) {
        DataType dataType;
        switch (type.toUpperCase()) {
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case BYTES:
                if (Decimal.LOGICAL_NAME.equals(className)) {
                    try {
                        BigDecimal temp = new BigDecimal(rawValue);
                        dataType = DataTypes.DECIMAL(temp.precision(), temp.scale());
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(
                                "Invalid big decimal value "
                                        + rawValue
                                        + ". Make sure that in the `customConverterConfigs` "
                                        + "of the JsonDebeziumDeserializationSchema you created, set '"
                                        + JsonConverterConfig.DECIMAL_FORMAT_CONFIG
                                        + "' to 'numeric'",
                                e);
                    }
                } else {
                    dataType = DataTypes.BYTES();
                }
                return dataType;
            case INT16:
                return DataTypes.SMALLINT();
            case INT32:
                if (Date.SCHEMA_NAME.equals(className)) {
                    return DataTypes.DATE();
                }
                return DataTypes.INT();
            case INT64:
                if (MicroTime.SCHEMA_NAME.equals(className)) {
                    dataType = DataTypes.TIME();
                } else if (Timestamp.SCHEMA_NAME.equals(className)) {
                    dataType = DataTypes.TIMESTAMP(3);
                } else if (MicroTimestamp.SCHEMA_NAME.equals(className)) {
                    dataType = DataTypes.TIMESTAMP(6);
                } else {
                    dataType = DataTypes.BIGINT();
                }

                return dataType;
            case FLOAT32:
                return DataTypes.FLOAT();
            case FLOAT64:
            case DOUBLE:
                return DataTypes.DOUBLE();
            case STRING:
                return DataTypes.STRING();
            default:
                throw new IllegalArgumentException("Unsupported debezium data type: " + type);
        }
    }
}
