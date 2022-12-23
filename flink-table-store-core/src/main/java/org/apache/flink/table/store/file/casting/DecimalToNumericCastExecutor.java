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
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.types.logical.LogicalType;

/** Cast decimal to numeric. */
public class DecimalToNumericCastExecutor implements CastExecutor<DecimalData, Number> {
    private final LogicalType outputType;

    public DecimalToNumericCastExecutor(LogicalType outputType) {
        this.outputType = outputType;
    }

    @Override
    public Number cast(DecimalData value) throws TableException {
        if (value == null) {
            return null;
        }

        switch (outputType.getTypeRoot()) {
            case TINYINT:
                {
                    return (byte) DecimalDataUtils.castToIntegral(value);
                }
            case SMALLINT:
                {
                    return (short) DecimalDataUtils.castToIntegral(value);
                }
            case INTEGER:
                {
                    return (int) DecimalDataUtils.castToIntegral(value);
                }
            case BIGINT:
                {
                    return DecimalDataUtils.castToIntegral(value);
                }
            case FLOAT:
                {
                    return (float) DecimalDataUtils.doubleValue(value);
                }
            case DOUBLE:
                {
                    return DecimalDataUtils.doubleValue(value);
                }
            default:
                {
                    throw new UnsupportedOperationException(
                            String.format("Cannot convert decimal to %s", outputType));
                }
        }
    }
}
