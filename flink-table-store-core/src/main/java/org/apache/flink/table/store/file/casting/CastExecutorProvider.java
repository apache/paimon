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

import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import javax.annotation.Nullable;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.BINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.CHAR;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARBINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;

/** Provider for {@link CastExecutor}. */
public class CastExecutorProvider {
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
                        case SMALLINT:
                        case INTEGER:
                        case BIGINT:
                        case FLOAT:
                        case DOUBLE:
                            {
                                return new NumericToNumericCastExecutor(inputType, outputType);
                            }
                        case DECIMAL:
                            {
                                DecimalType decimalType = (DecimalType) outputType;
                                return new NumericToDecimalCastExecutor(
                                        inputType,
                                        decimalType.getPrecision(),
                                        decimalType.getScale());
                            }
                        default:
                            {
                                return null;
                            }
                    }
                }
            case DECIMAL:
                {
                    switch (outputType.getTypeRoot()) {
                        case TINYINT:
                        case SMALLINT:
                        case INTEGER:
                        case BIGINT:
                        case FLOAT:
                        case DOUBLE:
                            {
                                return new DecimalToNumericCastExecutor(outputType);
                            }
                        case DECIMAL:
                            {
                                DecimalType decimalType = (DecimalType) outputType;
                                return new DecimalToDecimalCastExecutor(
                                        decimalType.getPrecision(), decimalType.getScale());
                            }
                        default:
                            {
                                return null;
                            }
                    }
                }
            case CHAR:
            case VARCHAR:
                {
                    if (outputType.getTypeRoot() == CHAR || outputType.getTypeRoot() == VARCHAR) {
                        return new StringToStringCastExecutor(
                                outputType.getTypeRoot() == CHAR, getStringLength(outputType));
                    } else if (outputType.getTypeRoot() == VARBINARY) {
                        return new StringToBinaryCastExecutor(getBinaryLength(outputType));
                    }
                    return null;
                }
            case BINARY:
                {
                    if (outputType.getTypeRoot() == BINARY) {
                        return new BinaryToBinaryCastExecutor(getBinaryLength(outputType));
                    }
                    return null;
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    switch (outputType.getTypeRoot()) {
                        case DATE:
                            {
                                return TimestampToDateCastExecutor.INSTANCE;
                            }
                        case TIMESTAMP_WITHOUT_TIME_ZONE:
                            {
                                return new TimestampToTimestampCastExecutor(
                                        ((TimestampType) outputType).getPrecision());
                            }
                        case TIME_WITHOUT_TIME_ZONE:
                            {
                                return TimestampToTimeCastExecutor.INSTANCE;
                            }
                        default:
                            {
                                return null;
                            }
                    }
                }
            case TIME_WITHOUT_TIME_ZONE:
                {
                    if (outputType.getTypeRoot() == TIMESTAMP_WITHOUT_TIME_ZONE) {
                        return TimestampToTimeCastExecutor.INSTANCE;
                    }
                    return null;
                }
            default:
                {
                    return null;
                }
        }
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
