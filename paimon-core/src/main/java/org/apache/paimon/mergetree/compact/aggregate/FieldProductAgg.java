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

    public FieldProductAgg(String name, DataType dataType) {
        super(name, dataType);
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
                product = multiplyExactByte((byte) accumulator, (byte) inputField);
                break;
            case SMALLINT:
                product = multiplyExactShort((short) accumulator, (short) inputField);
                break;
            case INTEGER:
                product = multiplyExactInt((int) accumulator, (int) inputField);
                break;
            case BIGINT:
                product = multiplyExactLong((long) accumulator, (long) inputField);
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

    private static byte multiplyExactByte(byte a, byte b) {
        int value = a * b;
        if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE) {
            throw new ArithmeticException(
                    String.format("byte overflow: %d * %d = %d", a, b, value));
        }
        return (byte) value;
    }

    private static short multiplyExactShort(short a, short b) {
        int value = a * b;
        if (value > Short.MAX_VALUE || value < Short.MIN_VALUE) {
            throw new ArithmeticException(
                    String.format("short overflow: %d * %d = %d", a, b, value));
        }
        return (short) value;
    }

    private static int multiplyExactInt(int a, int b) {
        try {
            return Math.multiplyExact(a, b);
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("int overflow: %d * %d", a, b));
        }
    }

    private static long multiplyExactLong(long a, long b) {
        try {
            return Math.multiplyExact(a, b);
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("long overflow: %d * %d", a, b));
        }
    }

    private static byte divideExactByte(byte a, byte b) {
        int value = a / b;
        if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE) {
            throw new ArithmeticException(
                    String.format("byte overflow: %d / %d = %d", a, b, value));
        }
        return (byte) value;
    }

    private static short divideExactShort(short a, short b) {
        int value = a / b;
        if (value > Short.MAX_VALUE || value < Short.MIN_VALUE) {
            throw new ArithmeticException(
                    String.format("short overflow: %d / %d = %d", a, b, value));
        }
        return (short) value;
    }

    private static int divideExactInt(int a, int b) {
        if (a == Integer.MIN_VALUE && b == -1) {
            throw new ArithmeticException(String.format("int overflow: %d / %d", a, b));
        }
        return a / b;
    }

    private static long divideExactLong(long a, long b) {
        if (a == Long.MIN_VALUE && b == -1L) {
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
                    product = divideExactByte((byte) accumulator, (byte) inputField);
                    break;
                case SMALLINT:
                    product = divideExactShort((short) accumulator, (short) inputField);
                    break;
                case INTEGER:
                    product = divideExactInt((int) accumulator, (int) inputField);
                    break;
                case BIGINT:
                    product = divideExactLong((long) accumulator, (long) inputField);
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
