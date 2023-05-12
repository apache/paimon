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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** TruncateComputer is used to truncate a numeric/decimal/string value to a specified width. */
public class TruncateComputer implements Expression {
    private static final long serialVersionUID = 1L;

    private final String fieldReference;

    private DataType fieldType;

    private int width;

    TruncateComputer(String fieldReference, DataType fieldType, String literal) {
        this.fieldReference = fieldReference;
        this.fieldType = fieldType;
        try {
            this.width = Integer.parseInt(literal);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid width value for truncate function: %s, expected integer.",
                            literal));
        }
    }

    @Override
    public String fieldReference() {
        return fieldReference;
    }

    @Override
    public DataType outputType() {
        return DataTypes.STRING();
    }

    @Override
    public String eval(String input) {
        switch (fieldType.getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
                return String.valueOf(truncateShort(width, Short.valueOf(input)));
            case INTEGER:
                return String.valueOf(truncateInt(width, Integer.valueOf(input)));
            case BIGINT:
                return String.valueOf(truncateLong(width, Long.valueOf(input)));
            case DECIMAL:
                return truncateDecimal(BigInteger.valueOf(width), new BigDecimal(input)).toString();
            case VARCHAR:
            case CHAR:
                checkArgument(
                        width <= input.length(),
                        "Invalid width value for truncate function: %s, expected less than or equal to %s.",
                        width,
                        input.length());
                return input.substring(0, width);
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported field type for truncate function: %s.",
                                fieldType.getTypeRoot().toString()));
        }
    }

    private short truncateShort(int width, short value) {
        return (short) (value - (((value % width) + width) % width));
    }

    private int truncateInt(int width, int value) {
        return value - (((value % width) + width) % width);
    }

    private long truncateLong(int width, long value) {
        return value - (((value % width) + width) % width);
    }

    private BigDecimal truncateDecimal(BigInteger unscaledWidth, BigDecimal value) {
        BigDecimal remainder =
                new BigDecimal(
                        value.unscaledValue()
                                .remainder(unscaledWidth)
                                .add(unscaledWidth)
                                .remainder(unscaledWidth),
                        value.scale());

        return value.subtract(remainder);
    }
}
