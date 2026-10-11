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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.types.DataType;

import java.math.BigDecimal;

import static org.apache.paimon.data.Decimal.fromBigDecimal;

/** product value aggregate a field of a row. */
public class FieldProductAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final boolean failOnOverflow;

    public FieldProductAgg(String name, DataType dataType, boolean failOnOverflow) {
        super(name, dataType);
        this.failOnOverflow = failOnOverflow;
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        Object product;

        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case DECIMAL:
                Decimal mergeFieldDD = (Decimal) accumulator;
                Decimal inFieldDD = (Decimal) inputField;
                assert mergeFieldDD.scale() == inFieldDD.scale()
                        : "Inconsistent scale of aggregate Decimal!";
                assert mergeFieldDD.precision() == inFieldDD.precision()
                        : "Inconsistent precision of aggregate Decimal!";
                BigDecimal bigDecimal = mergeFieldDD.toBigDecimal();
                BigDecimal bigDecimal1 = inFieldDD.toBigDecimal();
                BigDecimal mul = bigDecimal.multiply(bigDecimal1);
                product = fromBigDecimal(mul, mergeFieldDD.precision(), mergeFieldDD.scale());
                break;
            case TINYINT:
                product = multiplyByte((byte) accumulator, (byte) inputField);
                break;
            case SMALLINT:
                product = multiplyShort((short) accumulator, (short) inputField);
                break;
            case INTEGER:
                product = multiplyInt((int) accumulator, (int) inputField);
                break;
            case BIGINT:
                product = multiplyLong((long) accumulator, (long) inputField);
                break;
            case FLOAT:
                product = (float) accumulator * (float) inputField;
                break;
            case DOUBLE:
                product = (double) accumulator * (double) inputField;
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), this.getClass().getName());
                throw new IllegalArgumentException(msg);
        }
        return product;
    }

    private byte multiplyByte(byte a, byte b) {
        int value = a * b;
        if (failOnOverflow && (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE)) {
            throw new ArithmeticException(
                    String.format("byte overflow: %d * %d = %d", a, b, value));
        }
        return (byte) value;
    }

    private short multiplyShort(short a, short b) {
        int value = a * b;
        if (failOnOverflow && (value > Short.MAX_VALUE || value < Short.MIN_VALUE)) {
            throw new ArithmeticException(
                    String.format("short overflow: %d * %d = %d", a, b, value));
        }
        return (short) value;
    }

    private int multiplyInt(int a, int b) {
        try {
            return failOnOverflow ? Math.multiplyExact(a, b) : a * b;
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("int overflow: %d * %d", a, b));
        }
    }

    private long multiplyLong(long a, long b) {
        try {
            return failOnOverflow ? Math.multiplyExact(a, b) : a * b;
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("long overflow: %d * %d", a, b));
        }
    }

    private byte divideByte(byte a, byte b) {
        int value = a / b;
        if (failOnOverflow && (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE)) {
            throw new ArithmeticException(
                    String.format("byte overflow: %d / %d = %d", a, b, value));
        }
        return (byte) value;
    }

    private short divideShort(short a, short b) {
        int value = a / b;
        if (failOnOverflow && (value > Short.MAX_VALUE || value < Short.MIN_VALUE)) {
            throw new ArithmeticException(
                    String.format("short overflow: %d / %d = %d", a, b, value));
        }
        return (short) value;
    }

    private int divideInt(int a, int b) {
        if (failOnOverflow && a == Integer.MIN_VALUE && b == -1) {
            throw new ArithmeticException(String.format("int overflow: %d / %d", a, b));
        }
        return a / b;
    }

    private long divideLong(long a, long b) {
        if (failOnOverflow && a == Long.MIN_VALUE && b == -1L) {
            throw new ArithmeticException(String.format("long overflow: %d / %d", a, b));
        }
        return a / b;
    }

    @Override
    public Object retract(Object accumulator, Object inputField) {
        Object product;

        if (accumulator == null || inputField == null) {
            product = accumulator;
        } else {
            switch (fieldType.getTypeRoot()) {
                case DECIMAL:
                    Decimal mergeFieldDD = (Decimal) accumulator;
                    Decimal inFieldDD = (Decimal) inputField;
                    assert mergeFieldDD.scale() == inFieldDD.scale()
                            : "Inconsistent scale of aggregate Decimal!";
                    assert mergeFieldDD.precision() == inFieldDD.precision()
                            : "Inconsistent precision of aggregate Decimal!";
                    BigDecimal bigDecimal = mergeFieldDD.toBigDecimal();
                    BigDecimal bigDecimal1 = inFieldDD.toBigDecimal();
                    BigDecimal div = bigDecimal.divide(bigDecimal1);
                    product = fromBigDecimal(div, mergeFieldDD.precision(), mergeFieldDD.scale());
                    break;
                case TINYINT:
                    product = divideByte((byte) accumulator, (byte) inputField);
                    break;
                case SMALLINT:
                    product = divideShort((short) accumulator, (short) inputField);
                    break;
                case INTEGER:
                    product = divideInt((int) accumulator, (int) inputField);
                    break;
                case BIGINT:
                    product = divideLong((long) accumulator, (long) inputField);
                    break;
                case FLOAT:
                    product = (float) accumulator / (float) inputField;
                    break;
                case DOUBLE:
                    product = (double) accumulator / (double) inputField;
                    break;
                default:
                    String msg =
                            String.format(
                                    "type %s not support in %s",
                                    fieldType.getTypeRoot().toString(), this.getClass().getName());
                    throw new IllegalArgumentException(msg);
            }
        }
        return product;
    }
}
