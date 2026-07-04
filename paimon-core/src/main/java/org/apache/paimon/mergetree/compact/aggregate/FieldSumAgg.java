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
import org.apache.paimon.utils.DecimalUtils;

/** Sum aggregate a field of a row. */
public class FieldSumAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    public FieldSumAgg(String name, DataType dataType) {
        super(name, dataType);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }
        Object sum;

        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case DECIMAL:
                Decimal mergeFieldDD = (Decimal) accumulator;
                Decimal inFieldDD = (Decimal) inputField;
                assert mergeFieldDD.scale() == inFieldDD.scale()
                        : "Inconsistent scale of aggregate Decimal!";
                assert mergeFieldDD.precision() == inFieldDD.precision()
                        : "Inconsistent precision of aggregate Decimal!";
                sum =
                        DecimalUtils.add(
                                mergeFieldDD,
                                inFieldDD,
                                mergeFieldDD.precision(),
                                mergeFieldDD.scale());
                break;
            case TINYINT:
                sum = addExactByte((byte) accumulator, (byte) inputField);
                break;
            case SMALLINT:
                sum = addExactShort((short) accumulator, (short) inputField);
                break;
            case INTEGER:
                sum = addExactInt((int) accumulator, (int) inputField);
                break;
            case BIGINT:
                sum = addExactLong((long) accumulator, (long) inputField);
                break;
            case FLOAT:
                sum = (float) accumulator + (float) inputField;
                break;
            case DOUBLE:
                sum = (double) accumulator + (double) inputField;
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), this.getClass().getName());
                throw new IllegalArgumentException(msg);
        }
        return sum;
    }

    @Override
    public Object retract(Object accumulator, Object inputField) {

        if (accumulator == null || inputField == null) {
            return (accumulator == null ? negative(inputField) : accumulator);
        }
        Object sum;
        switch (fieldType.getTypeRoot()) {
            case DECIMAL:
                Decimal mergeFieldDD = (Decimal) accumulator;
                Decimal inFieldDD = (Decimal) inputField;
                assert mergeFieldDD.scale() == inFieldDD.scale()
                        : "Inconsistent scale of aggregate Decimal!";
                assert mergeFieldDD.precision() == inFieldDD.precision()
                        : "Inconsistent precision of aggregate Decimal!";
                sum =
                        DecimalUtils.subtract(
                                mergeFieldDD,
                                inFieldDD,
                                mergeFieldDD.precision(),
                                mergeFieldDD.scale());
                break;
            case TINYINT:
                sum = subtractExactByte((byte) accumulator, (byte) inputField);
                break;
            case SMALLINT:
                sum = subtractExactShort((short) accumulator, (short) inputField);
                break;
            case INTEGER:
                sum = subtractExactInt((int) accumulator, (int) inputField);
                break;
            case BIGINT:
                sum = subtractExactLong((long) accumulator, (long) inputField);
                break;
            case FLOAT:
                sum = (float) accumulator - (float) inputField;
                break;
            case DOUBLE:
                sum = (double) accumulator - (double) inputField;
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), this.getClass().getName());
                throw new IllegalArgumentException(msg);
        }
        return sum;
    }

    private Object negative(Object value) {
        if (value == null) {
            return null;
        }
        switch (fieldType.getTypeRoot()) {
            case DECIMAL:
                Decimal decimal = (Decimal) value;
                return Decimal.fromBigDecimal(
                        decimal.toBigDecimal().negate(), decimal.precision(), decimal.scale());
            case TINYINT:
                return negateExactByte((byte) value);
            case SMALLINT:
                return negateExactShort((short) value);
            case INTEGER:
                return negateExactInt((int) value);
            case BIGINT:
                return negateExactLong((long) value);
            case FLOAT:
                return -((float) value);
            case DOUBLE:
                return -((double) value);
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), this.getClass().getName());
                throw new IllegalArgumentException(msg);
        }
    }

    private static byte addExactByte(byte a, byte b) {
        int value = a + b;
        if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE) {
            throw new ArithmeticException(
                    String.format("byte overflow: %d + %d = %d", a, b, value));
        }
        return (byte) value;
    }

    private static short addExactShort(short a, short b) {
        int value = a + b;
        if (value > Short.MAX_VALUE || value < Short.MIN_VALUE) {
            throw new ArithmeticException(
                    String.format("short overflow: %d + %d = %d", a, b, value));
        }
        return (short) value;
    }

    private static int addExactInt(int a, int b) {
        try {
            return Math.addExact(a, b);
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("int overflow: %d + %d", a, b));
        }
    }

    private static long addExactLong(long a, long b) {
        try {
            return Math.addExact(a, b);
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("long overflow: %d + %d", a, b));
        }
    }

    private static byte subtractExactByte(byte a, byte b) {
        int value = a - b;
        if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE) {
            throw new ArithmeticException(
                    String.format("byte overflow: %d - %d = %d", a, b, value));
        }
        return (byte) value;
    }

    private static short subtractExactShort(short a, short b) {
        int value = a - b;
        if (value > Short.MAX_VALUE || value < Short.MIN_VALUE) {
            throw new ArithmeticException(
                    String.format("short overflow: %d - %d = %d", a, b, value));
        }
        return (short) value;
    }

    private static int subtractExactInt(int a, int b) {
        try {
            return Math.subtractExact(a, b);
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("int overflow: %d - %d", a, b));
        }
    }

    private static long subtractExactLong(long a, long b) {
        try {
            return Math.subtractExact(a, b);
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("long overflow: %d - %d", a, b));
        }
    }

    private static byte negateExactByte(byte a) {
        int value = -a;
        if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE) {
            throw new ArithmeticException(String.format("byte overflow: -%d = %d", a, value));
        }
        return (byte) value;
    }

    private static short negateExactShort(short a) {
        int value = -a;
        if (value > Short.MAX_VALUE || value < Short.MIN_VALUE) {
            throw new ArithmeticException(String.format("short overflow: -%d = %d", a, value));
        }
        return (short) value;
    }

    private static int negateExactInt(int a) {
        try {
            return Math.negateExact(a);
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("int overflow: -%d", a));
        }
    }

    private static long negateExactLong(long a) {
        try {
            return Math.negateExact(a);
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("long overflow: -%d", a));
        }
    }
}
