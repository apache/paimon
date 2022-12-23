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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.logical.LogicalType;

/** Cast numeric to numeric. */
public class NumericToNumericCastExecutor implements CastExecutor<Number, Number> {
    private final LogicalType inputType;
    private final LogicalType outputType;

    public NumericToNumericCastExecutor(LogicalType inputType, LogicalType outputType) {
        this.inputType = inputType;
        this.outputType = outputType;
    }

    @Override
    public Number cast(Number value) throws TableException {
        if (value == null) {
            return null;
        }

        switch (outputType.getTypeRoot()) {
            case TINYINT:
                {
                    return value.byteValue();
                }
            case SMALLINT:
                {
                    return value.shortValue();
                }
            case INTEGER:
                {
                    return value.intValue();
                }
            case BIGINT:
                {
                    return value.longValue();
                }
            case FLOAT:
                {
                    return value.floatValue();
                }
            case DOUBLE:
                {
                    return value.doubleValue();
                }
            default:
                {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Cannot convert value from type [%s] to [%s]",
                                    inputType.toString(), outputType.toString()));
                }
        }
    }
}
