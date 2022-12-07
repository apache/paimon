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

package org.apache.flink.table.store.file.schema;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.store.utils.DateTimeUtils;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.Date;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.BINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.CHAR;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DATE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARBINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;
import static org.apache.flink.util.Preconditions.checkState;

/** Data value converter for column type evolution. */
public class DataValueConverter {
    private final LogicalType sourceType;
    private final LogicalType targetType;

    public DataValueConverter(LogicalType sourceType, LogicalType targetType) {
        this.sourceType = sourceType;
        this.targetType = targetType;
    }

    /**
     * Convert the source value with {@link LogicalType} to target {@link LogicalType} according to
     * {@Code LogicalTypeCasts#implicitCastingRules}.
     *
     * @param sourceValue the source value
     * @return the target value
     */
    public Object convert(Object sourceValue) {
        checkState(
                LogicalTypeCasts.supportsImplicitCast(sourceType, targetType),
                String.format(
                        "Cant convert value[%s] from type[%s] to [%s] without loosing information",
                        sourceValue, sourceType, targetType));

        if (sourceType.equals(targetType) || sourceValue == null) {
            return sourceValue;
        }

        switch (targetType.getTypeRoot()) {
            case VARCHAR:
                {
                    // Convert from CHAR
                    if (sourceType.getTypeRoot() == VARCHAR) {
                        return sourceValue;
                    } else if (sourceType.getTypeRoot() == CHAR) {
                        return StringData.fromString(
                                StringUtils.stripEnd(sourceValue.toString(), " "));
                    }

                    throw new UnsupportedOperationException(
                            String.format(
                                    "Cannot convert value[%s] from %s to %s type",
                                    sourceType, sourceType, targetType));
                }
            case VARBINARY:
                {
                    // Convert from BINARY
                    if (sourceType.getTypeRoot() == VARBINARY
                            || sourceType.getTypeRoot() == BINARY) {
                        return sourceValue;
                    }

                    throw new UnsupportedOperationException(
                            String.format(
                                    "Cannot convert value[%s] from %s to %s type",
                                    sourceType, sourceType, targetType));
                }
            case DECIMAL:
                {
                    DecimalType decimalType = (DecimalType) targetType;
                    // Convert from TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE
                    switch (sourceType.getTypeRoot()) {
                        case DECIMAL:
                            {
                                return DecimalData.fromBigDecimal(
                                        getDecimal(sourceValue),
                                        decimalType.getPrecision(),
                                        decimalType.getScale());
                            }
                        case TINYINT:
                            {
                                return DecimalData.fromBigDecimal(
                                        new BigDecimal(getByte(sourceValue)),
                                        decimalType.getPrecision(),
                                        decimalType.getScale());
                            }
                        case SMALLINT:
                            {
                                return DecimalData.fromBigDecimal(
                                        new BigDecimal(getShort(sourceValue)),
                                        decimalType.getPrecision(),
                                        decimalType.getScale());
                            }
                        case INTEGER:
                            {
                                return DecimalData.fromBigDecimal(
                                        new BigDecimal(getInt(sourceValue)),
                                        decimalType.getPrecision(),
                                        decimalType.getScale());
                            }
                        case BIGINT:
                            {
                                return DecimalData.fromBigDecimal(
                                        new BigDecimal(getLong(sourceValue)),
                                        decimalType.getPrecision(),
                                        decimalType.getScale());
                            }
                        case FLOAT:
                            {
                                return DecimalData.fromBigDecimal(
                                        BigDecimal.valueOf(getFloat(sourceValue)),
                                        decimalType.getPrecision(),
                                        decimalType.getScale());
                            }
                        case DOUBLE:
                            {
                                return DecimalData.fromBigDecimal(
                                        BigDecimal.valueOf(getDouble(sourceValue)),
                                        decimalType.getPrecision(),
                                        decimalType.getScale());
                            }
                    }

                    throw new UnsupportedOperationException(
                            String.format(
                                    "Cannot convert value[%s] from %s to %s type",
                                    sourceType, sourceType, targetType));
                }
            case SMALLINT:
                {
                    // Convert from TINYINT
                    switch (sourceType.getTypeRoot()) {
                        case TINYINT:
                            return (short) getByte(sourceValue);
                        case SMALLINT:
                            return getShort(sourceValue);
                    }

                    throw new UnsupportedOperationException(
                            String.format(
                                    "Cannot convert value[%s] from %s to %s type",
                                    sourceType, sourceType, targetType));
                }
            case INTEGER:
                {
                    // Convert from TINYINT, SMALLINT
                    switch (sourceType.getTypeRoot()) {
                        case TINYINT:
                            return (int) getByte(sourceValue);
                        case SMALLINT:
                            return (int) getShort(sourceValue);
                        case INTEGER:
                            return getInt(sourceValue);
                    }

                    throw new UnsupportedOperationException(
                            String.format(
                                    "Cannot convert value[%s] from %s to %s type",
                                    sourceType, sourceType, targetType));
                }
            case BIGINT:
                {
                    // Convert from logical types TINYINT, SMALLINT, INTEGER
                    switch (sourceType.getTypeRoot()) {
                        case TINYINT:
                            return (long) getByte(sourceValue);
                        case SMALLINT:
                            return (long) getShort(sourceValue);
                        case INTEGER:
                            return (long) getInt(sourceValue);
                        case BIGINT:
                            return getLong(sourceValue);
                    }

                    throw new UnsupportedOperationException(
                            String.format(
                                    "Cannot convert value[%s] from %s to %s type",
                                    sourceType, sourceType, targetType));
                }
            case FLOAT:
                {
                    // Convert from TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DECIMAL
                    switch (sourceType.getTypeRoot()) {
                        case TINYINT:
                            return (float) getByte(sourceValue);
                        case SMALLINT:
                            return (float) getShort(sourceValue);
                        case INTEGER:
                            return (float) getInt(sourceValue);
                        case BIGINT:
                            return (float) getLong(sourceValue);
                        case FLOAT:
                            return getFloat(sourceValue);
                        case DECIMAL:
                            return getDecimal(sourceValue).floatValue();
                    }

                    throw new UnsupportedOperationException(
                            String.format(
                                    "Cannot convert value[%s] from %s to %s type",
                                    sourceType, sourceType, targetType));
                }
            case DOUBLE:
                {
                    // Convert from TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DECIMAL
                    switch (sourceType.getTypeRoot()) {
                        case TINYINT:
                            return (double) getByte(sourceValue);
                        case SMALLINT:
                            return (double) getShort(sourceValue);
                        case INTEGER:
                            return (double) getInt(sourceValue);
                        case BIGINT:
                            return (double) getLong(sourceValue);
                        case FLOAT:
                            return (double) getFloat(sourceValue);
                        case DOUBLE:
                            return getDouble(sourceValue);
                        case DECIMAL:
                            return getDecimal(sourceValue).doubleValue();
                    }

                    throw new UnsupportedOperationException(
                            String.format(
                                    "Cannot convert value[%s] from %s to %s type",
                                    sourceType, sourceType, targetType));
                }
            case DATE:
                {
                    // Convert from TIMESTAMP_WITHOUT_TIME_ZONE
                    if (sourceType.getTypeRoot() == DATE) {
                        return sourceValue;
                    } else if (sourceType.getTypeRoot() == TIMESTAMP_WITHOUT_TIME_ZONE) {
                        TimestampData timestampData = (TimestampData) sourceValue;
                        return DateTimeUtils.toInternal(
                                new Date(timestampData.toTimestamp().getTime()));
                    }

                    throw new UnsupportedOperationException(
                            String.format(
                                    "Cannot convert value[%s] from %s to %s type",
                                    sourceType, sourceType, targetType));
                }
                /**
                 * TODO Convert from TIMESTAMP_WITH_TIME_ZONE which is not supported
                 * TIMESTAMP_WITH_TIME_ZONE yet
                 */
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    // TODO not supported logical type
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Cannot convert value[%s] from %s to %s type",
                                    sourceType, sourceType, targetType));
                }
        }

        throw new UnsupportedOperationException(
                String.format(
                        "Cannot convert value[%s] from %s to %s type",
                        sourceType, sourceType, targetType));
    }

    private byte getByte(Object val) {
        return (byte) val;
    }

    private short getShort(Object val) {
        return (short) val;
    }

    private int getInt(Object val) {
        return (int) val;
    }

    private long getLong(Object val) {
        return (long) val;
    }

    private float getFloat(Object val) {
        return (float) val;
    }

    private double getDouble(Object val) {
        return (double) val;
    }

    private BigDecimal getDecimal(Object val) {
        DecimalData decimalData = (DecimalData) val;
        return decimalData.toBigDecimal();
    }
}
