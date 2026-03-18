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

/** average aggregate a field of a row. */
public class FieldAvgAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    public FieldAvgAgg(String name, DataType dataType) {
        super(name, dataType);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (inputField == null) {
            return accumulator;
        }
        if (accumulator == null) {
            return createAccumulator(inputField);
        }

        Accumulator acc = (Accumulator) accumulator;
        acc.sum = add(acc.sum, inputField);
        acc.count++;
        return acc;
    }

    @Override
    public Object retract(Object accumulator, Object inputField) {
        if (inputField == null) {
            return accumulator;
        }
        if (accumulator == null) {
            throw new IllegalArgumentException("Cannot retract from null accumulator");
        }

        Accumulator acc = (Accumulator) accumulator;
        acc.sum = subtract(acc.sum, inputField);
        acc.count--;
        return acc;
    }

    @Override
    public Object aggReversed(Object accumulator, Object inputField) {
        return agg(inputField, accumulator);
    }

    private Object add(Object a, Object b) {
        switch (fieldType.getTypeRoot()) {
            case DECIMAL:
                Decimal d1 = (Decimal) a;
                Decimal d2 = (Decimal) b;
                assert d1.scale() == d2.scale() : "Inconsistent scale of aggregate Decimal!";
                assert d1.precision() == d2.precision()
                        : "Inconsistent precision of aggregate Decimal!";
                return DecimalUtils.add(d1, d2, d1.precision(), d1.scale());
            case TINYINT:
                return (byte) ((byte) a + (byte) b);
            case SMALLINT:
                return (short) ((short) a + (short) b);
            case INTEGER:
                return (int) a + (int) b;
            case BIGINT:
                return (long) a + (long) b;
            case FLOAT:
                return (float) a + (float) b;
            case DOUBLE:
                return (double) a + (double) b;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), this.getClass().getName()));
        }
    }

    private Object subtract(Object a, Object b) {
        switch (fieldType.getTypeRoot()) {
            case DECIMAL:
                Decimal d1 = (Decimal) a;
                Decimal d2 = (Decimal) b;
                assert d1.scale() == d2.scale() : "Inconsistent scale of aggregate Decimal!";
                assert d1.precision() == d2.precision()
                        : "Inconsistent precision of aggregate Decimal!";
                return DecimalUtils.subtract(d1, d2, d1.precision(), d1.scale());
            case TINYINT:
                return (byte) ((byte) a - (byte) b);
            case SMALLINT:
                return (short) ((short) a - (short) b);
            case INTEGER:
                return (int) a - (int) b;
            case BIGINT:
                return (long) a - (long) b;
            case FLOAT:
                return (float) a - (float) b;
            case DOUBLE:
                return (double) a - (double) b;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), this.getClass().getName()));
        }
    }

    private Accumulator createAccumulator(Object value) {
        Accumulator acc = new Accumulator();
        acc.sum = value;
        acc.count = 1;
        return acc;
    }

    /** Accumulator for average aggregation. */
    public static class Accumulator {
        public Object sum;
        public int count;

        public Accumulator() {}

        public Accumulator(Object sum, int count) {
            this.sum = sum;
            this.count = count;
        }
    }
}
