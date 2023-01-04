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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.BinaryStringDataUtil;
import org.apache.flink.table.store.utils.DateTimeUtils;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import javax.annotation.Nullable;

import java.util.Arrays;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.BINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.CHAR;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARBINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;

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
    public static @Nullable CastExecutor<?, ?> resolve(
            LogicalType inputType, LogicalType outputType) {
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
                                            return DecimalDataUtils.castFrom(
                                                    number.longValue(),
                                                    decimalType.getPrecision(),
                                                    decimalType.getScale());
                                        }
                                    default:
                                        {
                                            return DecimalDataUtils.castFrom(
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
                            return value ->
                                    (byte) DecimalDataUtils.castToIntegral((DecimalData) value);
                        case SMALLINT:
                            return value ->
                                    (short) DecimalDataUtils.castToIntegral((DecimalData) value);
                        case INTEGER:
                            return value ->
                                    (int) DecimalDataUtils.castToIntegral((DecimalData) value);
                        case BIGINT:
                            return value -> DecimalDataUtils.castToIntegral((DecimalData) value);
                        case FLOAT:
                            return value ->
                                    (float) DecimalDataUtils.doubleValue((DecimalData) value);
                        case DOUBLE:
                            return value -> DecimalDataUtils.doubleValue((DecimalData) value);
                        case DECIMAL:
                            DecimalType decimalType = (DecimalType) outputType;
                            return value ->
                                    DecimalDataUtils.castToDecimal(
                                            (DecimalData) value,
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
                        StringData result;
                        String strVal = value.toString();
                        BinaryStringData strData = BinaryStringData.fromString(strVal);
                        if (strData.numChars() > targetLength) {
                            result = BinaryStringData.fromString(strVal.substring(0, targetLength));
                        } else {
                            if (strData.numChars() < targetLength) {
                                if (targetCharType) {
                                    int padLength = targetLength - strData.numChars();
                                    BinaryStringData padString =
                                            BinaryStringData.blankString(padLength);
                                    result = BinaryStringDataUtil.concat(strData, padString);
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
                        byte[] byteArrayTerm = ((StringData) value).toBytes();
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
                                            (((TimestampData) value).getMillisecond()
                                                    / DateTimeUtils.MILLIS_PER_DAY);
                        }
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                        {
                            return value ->
                                    DateTimeUtils.truncate(
                                            (TimestampData) value,
                                            ((TimestampType) outputType).getPrecision());
                        }
                    case TIME_WITHOUT_TIME_ZONE:
                        {
                            return value ->
                                    (int)
                                            (((TimestampData) value).getMillisecond()
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
                                    (((TimestampData) value).getMillisecond()
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

    private static int getStringLength(LogicalType logicalType) {
        if (logicalType instanceof CharType) {
            return ((CharType) logicalType).getLength();
        } else if (logicalType instanceof VarCharType) {
            return ((VarCharType) logicalType).getLength();
        }

        throw new IllegalArgumentException(String.format("Unsupported type %s", logicalType));
    }

    private static int getBinaryLength(LogicalType logicalType) {
        if (logicalType instanceof VarBinaryType) {
            return ((VarBinaryType) logicalType).getLength();
        } else if (logicalType instanceof BinaryType) {
            return ((BinaryType) logicalType).getLength();
        }

        throw new IllegalArgumentException(String.format("Unsupported type %s", logicalType));
    }
}
