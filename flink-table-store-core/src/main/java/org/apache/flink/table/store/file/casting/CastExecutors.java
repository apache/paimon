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

package org.apache.flink.table.store.file.casting;

import org.apache.flink.table.store.data.BinaryString;
import org.apache.flink.table.store.data.Decimal;
import org.apache.flink.table.store.data.Timestamp;
import org.apache.flink.table.store.types.BinaryType;
import org.apache.flink.table.store.types.CharType;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.DecimalType;
import org.apache.flink.table.store.types.TimestampType;
import org.apache.flink.table.store.types.VarBinaryType;
import org.apache.flink.table.store.types.VarCharType;
import org.apache.flink.table.store.utils.DateTimeUtils;
import org.apache.flink.table.store.utils.DecimalUtils;
import org.apache.flink.table.store.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.Arrays;

import static org.apache.flink.table.store.types.DataTypeRoot.BINARY;
import static org.apache.flink.table.store.types.DataTypeRoot.CHAR;
import static org.apache.flink.table.store.types.DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.store.types.DataTypeRoot.VARBINARY;
import static org.apache.flink.table.store.types.DataTypeRoot.VARCHAR;

/** Cast executors for input type and output type. */
public class CastExecutors {
    private static final CastExecutor<?, ?> IDENTITY_CAST_EXECUTOR = value -> value;

    /**
     * Resolve a {@link CastExecutor} for the provided input type and target type. Returns null if
     * no rule can be resolved.
     *
     * @param inputType the input value type.
     * @param outputType the output value type.
     * @return the {@link CastExecutor} instance.
     */
    public static @Nullable CastExecutor<?, ?> resolve(DataType inputType, DataType outputType) {
        switch (inputType.getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                {
                    switch (outputType.getTypeRoot()) {
                        case TINYINT:
                            return value -> ((Number) value).byteValue();
                        case SMALLINT:
                            return value -> ((Number) value).shortValue();
                        case INTEGER:
                            return value -> ((Number) value).intValue();
                        case BIGINT:
                            return value -> ((Number) value).longValue();
                        case FLOAT:
                            return value -> ((Number) value).floatValue();
                        case DOUBLE:
                            return value -> ((Number) value).doubleValue();
                        case DECIMAL:
                            final DecimalType decimalType = (DecimalType) outputType;
                            return value -> {
                                final Number number = (Number) value;
                                switch (inputType.getTypeRoot()) {
                                    case TINYINT:
                                    case SMALLINT:
                                    case INTEGER:
                                    case BIGINT:
                                        {
                                            return DecimalUtils.castFrom(
                                                    number.longValue(),
                                                    decimalType.getPrecision(),
                                                    decimalType.getScale());
                                        }
                                    default:
                                        {
                                            return DecimalUtils.castFrom(
                                                    number.doubleValue(),
                                                    decimalType.getPrecision(),
                                                    decimalType.getScale());
                                        }
                                }
                            };
                        default:
                            return null;
                    }
                }
            case DECIMAL:
                {
                    switch (outputType.getTypeRoot()) {
                        case TINYINT:
                            return value -> (byte) DecimalUtils.castToIntegral((Decimal) value);
                        case SMALLINT:
                            return value -> (short) DecimalUtils.castToIntegral((Decimal) value);
                        case INTEGER:
                            return value -> (int) DecimalUtils.castToIntegral((Decimal) value);
                        case BIGINT:
                            return value -> DecimalUtils.castToIntegral((Decimal) value);
                        case FLOAT:
                            return value -> (float) DecimalUtils.doubleValue((Decimal) value);
                        case DOUBLE:
                            return value -> DecimalUtils.doubleValue((Decimal) value);
                        case DECIMAL:
                            DecimalType decimalType = (DecimalType) outputType;
                            return value ->
                                    DecimalUtils.castToDecimal(
                                            (Decimal) value,
                                            decimalType.getPrecision(),
                                            decimalType.getScale());
                        default:
                            return null;
                    }
                }
            case CHAR:
            case VARCHAR:
                if (outputType.getTypeRoot() == CHAR || outputType.getTypeRoot() == VARCHAR) {
                    final boolean targetCharType = outputType.getTypeRoot() == CHAR;
                    final int targetLength = getStringLength(outputType);
                    return value -> {
                        BinaryString result;
                        String strVal = value.toString();
                        BinaryString strData = BinaryString.fromString(strVal);
                        if (strData.numChars() > targetLength) {
                            result = BinaryString.fromString(strVal.substring(0, targetLength));
                        } else {
                            if (strData.numChars() < targetLength) {
                                if (targetCharType) {
                                    int padLength = targetLength - strData.numChars();
                                    BinaryString padString = BinaryString.blankString(padLength);
                                    result = StringUtils.concat(strData, padString);
                                } else {
                                    result = strData;
                                }
                            } else {
                                result = strData;
                            }
                        }

                        return result;
                    };
                } else if (outputType.getTypeRoot() == VARBINARY) {
                    final int targetLength = getBinaryLength(outputType);
                    return value -> {
                        byte[] byteArrayTerm = ((BinaryString) value).toBytes();
                        if (byteArrayTerm.length <= targetLength) {
                            return byteArrayTerm;
                        } else {
                            return Arrays.copyOf(byteArrayTerm, targetLength);
                        }
                    };
                }
                return null;
            case BINARY:
                if (outputType.getTypeRoot() == BINARY || outputType.getTypeRoot() == VARBINARY) {
                    boolean targetBinaryType = outputType.getTypeRoot() == BINARY;
                    final int targetLength = getBinaryLength(outputType);
                    return value -> {
                        byte[] bytes = (byte[]) value;
                        if (((byte[]) value).length == targetLength) {
                            return value;
                        }
                        if (targetBinaryType) {
                            if (bytes.length == targetLength) {
                                return bytes;
                            } else {
                                return Arrays.copyOf(bytes, targetLength);
                            }
                        } else {
                            if (bytes.length <= targetLength) {
                                return bytes;
                            } else {
                                return Arrays.copyOf(bytes, targetLength);
                            }
                        }
                    };
                }
                return null;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                switch (outputType.getTypeRoot()) {
                    case DATE:
                        {
                            return value ->
                                    (int)
                                            (((Timestamp) value).getMillisecond()
                                                    / DateTimeUtils.MILLIS_PER_DAY);
                        }
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                        {
                            return value ->
                                    DateTimeUtils.truncate(
                                            (Timestamp) value,
                                            ((TimestampType) outputType).getPrecision());
                        }
                    case TIME_WITHOUT_TIME_ZONE:
                        {
                            return value ->
                                    (int)
                                            (((Timestamp) value).getMillisecond()
                                                    % DateTimeUtils.MILLIS_PER_DAY);
                        }
                    default:
                        {
                            return null;
                        }
                }
            case TIME_WITHOUT_TIME_ZONE:
                if (outputType.getTypeRoot() == TIMESTAMP_WITHOUT_TIME_ZONE) {
                    return value ->
                            (int)
                                    (((Timestamp) value).getMillisecond()
                                            % DateTimeUtils.MILLIS_PER_DAY);
                }
                return null;
            default:
                return null;
        }
    }

    public static CastExecutor<?, ?> identityCastExecutor() {
        return IDENTITY_CAST_EXECUTOR;
    }

    private static int getStringLength(DataType dataType) {
        if (dataType instanceof CharType) {
            return ((CharType) dataType).getLength();
        } else if (dataType instanceof VarCharType) {
            return ((VarCharType) dataType).getLength();
        }

        throw new IllegalArgumentException(String.format("Unsupported type %s", dataType));
    }

    private static int getBinaryLength(DataType dataType) {
        if (dataType instanceof VarBinaryType) {
            return ((VarBinaryType) dataType).getLength();
        } else if (dataType instanceof BinaryType) {
            return ((BinaryType) dataType).getLength();
        }

        throw new IllegalArgumentException(String.format("Unsupported type %s", dataType));
    }
}
