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

/** sum aggregate a field of a row. */
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
                sum = (byte) ((byte) accumulator + (byte) inputField);
                break;
            case SMALLINT:
                sum = (short) ((short) accumulator + (short) inputField);
                break;
            case INTEGER:
                sum = (int) accumulator + (int) inputField;
                break;
            case BIGINT:
                sum = (long) accumulator + (long) inputField;
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
                sum = (byte) ((byte) accumulator - (byte) inputField);
                break;
            case SMALLINT:
                sum = (short) ((short) accumulator - (short) inputField);
                break;
            case INTEGER:
                sum = (int) accumulator - (int) inputField;
                break;
            case BIGINT:
                sum = (long) accumulator - (long) inputField;
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
                return (byte) -((byte) value);
            case SMALLINT:
                return (short) -((short) value);
            case INTEGER:
                return -((int) value);
            case BIGINT:
                return -((long) value);
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
}
