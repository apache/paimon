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

/** Cast numeric to decimal. */
public class NumericToDecimalCastExecutor implements CastExecutor<Number, DecimalData> {
    private final LogicalType inputType;
    private final int precision;
    private final int scale;

    public NumericToDecimalCastExecutor(LogicalType inputType, int precision, int scale) {
        this.inputType = inputType;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public DecimalData cast(Number value) throws TableException {
        if (value == null) {
            return null;
        }

        switch (inputType.getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                {
                    return DecimalDataUtils.castFrom(value.longValue(), precision, scale);
                }
            default:
                {
                    return DecimalDataUtils.castFrom(value.doubleValue(), precision, scale);
                }
        }
    }
}
