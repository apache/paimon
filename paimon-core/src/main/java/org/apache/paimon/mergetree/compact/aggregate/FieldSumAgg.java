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

    private final boolean failOnOverflow;

    public FieldSumAgg(String name, DataType dataType, boolean failOnOverflow) {
        super(name, dataType);
        this.failOnOverflow = failOnOverflow;
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
                sum = addByte((byte) accumulator, (byte) inputField);
                break;
            case SMALLINT:
                sum = addShort((short) accumulator, (short) inputField);
                break;
            case INTEGER:
                sum = addInt((int) accumulator, (int) inputField);
                break;
            case BIGINT:
                sum = addLong((long) accumulator, (long) inputField);
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
                sum = subtractByte((byte) accumulator, (byte) inputField);
                break;
            case SMALLINT:
                sum = subtractShort((short) accumulator, (short) inputField);
                break;
            case INTEGER:
                sum = subtractInt((int) accumulator, (int) inputField);
                break;
            case BIGINT:
                sum = subtractLong((long) accumulator, (long) inputField);
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
                return negateByte((byte) value);
            case SMALLINT:
                return negateShort((short) value);
            case INTEGER:
                return negateInt((int) value);
            case BIGINT:
                return negateLong((long) value);
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

    private byte addByte(byte a, byte b) {
        int value = a + b;
        if (failOnOverflow && (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE)) {
            throw new ArithmeticException(
                    String.format("byte overflow: %d + %d = %d", a, b, value));
        }
        return (byte) value;
    }

    private short addShort(short a, short b) {
        int value = a + b;
        if (failOnOverflow && (value > Short.MAX_VALUE || value < Short.MIN_VALUE)) {
            throw new ArithmeticException(
                    String.format("short overflow: %d + %d = %d", a, b, value));
        }
        return (short) value;
    }

    private int addInt(int a, int b) {
        try {
            return failOnOverflow ? Math.addExact(a, b) : a + b;
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("int overflow: %d + %d", a, b));
        }
    }

    private long addLong(long a, long b) {
        try {
            return failOnOverflow ? Math.addExact(a, b) : a + b;
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("long overflow: %d + %d", a, b));
        }
    }

    private byte subtractByte(byte a, byte b) {
        int value = a - b;
        if (failOnOverflow && (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE)) {
            throw new ArithmeticException(
                    String.format("byte overflow: %d - %d = %d", a, b, value));
        }
        return (byte) value;
    }

    private short subtractShort(short a, short b) {
        int value = a - b;
        if (failOnOverflow && (value > Short.MAX_VALUE || value < Short.MIN_VALUE)) {
            throw new ArithmeticException(
                    String.format("short overflow: %d - %d = %d", a, b, value));
        }
        return (short) value;
    }

    private int subtractInt(int a, int b) {
        try {
            return failOnOverflow ? Math.subtractExact(a, b) : a - b;
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("int overflow: %d - %d", a, b));
        }
    }

    private long subtractLong(long a, long b) {
        try {
            return failOnOverflow ? Math.subtractExact(a, b) : a - b;
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("long overflow: %d - %d", a, b));
        }
    }

    private byte negateByte(byte a) {
        int value = -a;
        if (failOnOverflow && (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE)) {
            throw new ArithmeticException(String.format("byte overflow: -%d = %d", a, value));
        }
        return (byte) value;
    }

    private short negateShort(short a) {
        int value = -a;
        if (failOnOverflow && (value > Short.MAX_VALUE || value < Short.MIN_VALUE)) {
            throw new ArithmeticException(String.format("short overflow: -%d = %d", a, value));
        }
        return (short) value;
    }

    private int negateInt(int a) {
        try {
            return failOnOverflow ? Math.negateExact(a) : -a;
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("int overflow: -%d", a));
        }
    }

    private long negateLong(long a) {
        try {
            return failOnOverflow ? Math.negateExact(a) : -a;
        } catch (ArithmeticException e) {
            throw new ArithmeticException(String.format("long overflow: -%d", a));
        }
    }
}
