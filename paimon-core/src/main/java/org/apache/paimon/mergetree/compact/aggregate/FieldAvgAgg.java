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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.types.DataType;

import java.math.BigDecimal;
import java.math.RoundingMode;

/** avg aggregate a field of a row. */
public class FieldAvgAgg extends FieldAggregator {

    public static final String NAME = "avg";
    private int count;
    private BigDecimal sum;

    public FieldAvgAgg(DataType dataType) {
        super(dataType);
        sum = BigDecimal.ZERO;
    }

    @Override
    String name() {
        return NAME;
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        Object average;
        if (inputField == null) {
            return accumulator;
        }
        count++;
        switch (fieldType.getTypeRoot()) {
            case DECIMAL:
                sum = sum.add((BigDecimal) inputField);
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP);
                break;
            case TINYINT:
                sum = sum.add(BigDecimal.valueOf((byte) inputField));
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP).byteValue();
                break;
            case SMALLINT:
                sum = sum.add(BigDecimal.valueOf((short) inputField));
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP).shortValue();
                break;
            case INTEGER:
                sum = sum.add(BigDecimal.valueOf((int) inputField));
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP).intValue();
                break;
            case BIGINT:
                sum = sum.add(BigDecimal.valueOf((long) inputField));
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP).longValue();
                break;
            case FLOAT:
                sum = sum.add(BigDecimal.valueOf((float) inputField));
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP).floatValue();
                break;
            case DOUBLE:
                sum = sum.add(BigDecimal.valueOf((double) inputField));
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP).doubleValue();
                break;
            default:
                throw new IllegalArgumentException();
        }

        return average;
    }
}
