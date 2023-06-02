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

package org.apache.paimon.casting;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.DecimalUtils;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.utils.TypeUtils;

import javax.annotation.Nullable;

import java.util.Arrays;

import static org.apache.paimon.types.DataTypeRoot.BINARY;
import static org.apache.paimon.types.DataTypeRoot.CHAR;
import static org.apache.paimon.types.DataTypeRoot.VARBINARY;

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
                        case CHAR:
                        case VARCHAR:
                            return value -> toCharacterString(value.toString(), outputType);
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
                        case CHAR:
                        case VARCHAR:
                            return value -> toCharacterString(value.toString(), outputType);
                        default:
                            return null;
                    }
                }
            case BOOLEAN:
                {
                    switch (outputType.getTypeRoot()) {
                        case TINYINT:
                            return value -> (byte) TypeUtils.toInt((Boolean) value);
                        case SMALLINT:
                            return value -> (short) TypeUtils.toInt((Boolean) value);
                        case INTEGER:
                            return value -> TypeUtils.toInt((Boolean) value);
                        case BIGINT:
                            return value -> (long) TypeUtils.toInt((Boolean) value);
                        case FLOAT:
                            return value -> (float) TypeUtils.toInt((Boolean) value);
                        case DOUBLE:
                            return value -> (double) TypeUtils.toInt((Boolean) value);
                        case DECIMAL:
                            final DecimalType decimalType = (DecimalType) outputType;
                            return value ->
                                    DecimalUtils.castFrom(
                                            TypeUtils.toInt((Boolean) value),
                                            decimalType.getPrecision(),
                                            decimalType.getScale());
                        case CHAR:
                        case VARCHAR:
                            return value -> toCharacterString(value.toString(), outputType);
                        default:
                            return null;
                    }
                }
            case CHAR:
            case VARCHAR:
                {
                    switch (outputType.getTypeRoot()) {
                        case CHAR:
                        case VARCHAR:
                            return value -> toCharacterString(value.toString(), outputType);
                        case VARBINARY:
                            {
                                final int targetLength = DataTypeChecks.getLength(outputType);
                                return value -> {
                                    byte[] byteArrayTerm = ((BinaryString) value).toBytes();
                                    if (byteArrayTerm.length <= targetLength) {
                                        return byteArrayTerm;
                                    } else {
                                        return Arrays.copyOf(byteArrayTerm, targetLength);
                                    }
                                };
                            }
                        case BOOLEAN:
                        case TINYINT:
                        case SMALLINT:
                        case INTEGER:
                        case BIGINT:
                        case FLOAT:
                        case DOUBLE:
                        case DECIMAL:
                        case DATE:
                        case TIME_WITHOUT_TIME_ZONE:
                        case TIMESTAMP_WITHOUT_TIME_ZONE:
                            return value -> TypeUtils.castFromString(value.toString(), outputType);
                        default:
                            return null;
                    }
                }
            case BINARY:
            case VARBINARY:
                if (outputType.getTypeRoot() == BINARY || outputType.getTypeRoot() == VARBINARY) {
                    boolean targetBinaryType = outputType.getTypeRoot() == BINARY;
                    final int targetLength = DataTypeChecks.getLength(outputType);
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
                    case TINYINT:
                        return value -> (byte) (((Timestamp) value).getMillisecond());
                    case SMALLINT:
                        return value -> (short) (((Timestamp) value).getMillisecond());
                    case INTEGER:
                        return value -> (int) (((Timestamp) value).getMillisecond());
                    case BIGINT:
                        return value -> (((Timestamp) value).getMillisecond());
                    case FLOAT:
                        return value -> (float) (((Timestamp) value).getMillisecond());
                    case DOUBLE:
                        return value -> (double) (((Timestamp) value).getMillisecond());
                    case DECIMAL:
                        final DecimalType decimalType = (DecimalType) outputType;
                        return value ->
                                DecimalUtils.castFrom(
                                        (((Timestamp) value).getMillisecond()),
                                        decimalType.getPrecision(),
                                        decimalType.getScale());
                    case CHAR:
                    case VARCHAR:
                        return value -> toCharacterString(value.toString(), outputType);
                    default:
                        return null;
                }
            case TIME_WITHOUT_TIME_ZONE:
                switch (outputType.getTypeRoot()) {
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                        return value ->
                                (int)
                                        (((Timestamp) value).getMillisecond()
                                                % DateTimeUtils.MILLIS_PER_DAY);
                    case CHAR:
                    case VARCHAR:
                        return value -> toCharacterString(value.toString(), outputType);
                    default:
                        return null;
                }
            case DATE:
                switch (outputType.getTypeRoot()) {
                    case CHAR:
                    case VARCHAR:
                        return value -> toCharacterString(value.toString(), outputType);
                    default:
                        return null;
                }
            default:
                return null;
        }
    }

    public static CastExecutor<?, ?> identityCastExecutor() {
        return IDENTITY_CAST_EXECUTOR;
    }

    private static BinaryString toCharacterString(String strVal, DataType type) {
        final boolean targetCharType = type.getTypeRoot() == CHAR;
        final int targetLength = DataTypeChecks.getLength(type);
        BinaryString strData = BinaryString.fromString(strVal);
        if (strData.numChars() > targetLength) {
            return BinaryString.fromString(strVal.substring(0, targetLength));
        } else if (strData.numChars() < targetLength && targetCharType) {
            int padLength = targetLength - strData.numChars();
            BinaryString padString = BinaryString.blankString(padLength);
            return StringUtils.concat(strData, padString);
        }
        return strData;
    }
}
