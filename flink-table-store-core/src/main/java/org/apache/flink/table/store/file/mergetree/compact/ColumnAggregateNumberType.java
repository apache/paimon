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

package org.apache.flink.table.store.file.mergetree.compact;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;

import java.io.Serializable;

/**
 * Supports numerical operations of different data types to facilitate the use of column
 * aggregation.
 *
 * @param <T> Corresponding data types in java.
 */
public interface ColumnAggregateNumberType<T> extends Serializable, Comparable<T> {
    T reset();

    T add(Object a);

    T subtract(Object a);

    Double getAvg(Long count);

    T getValue();
}

class DoubleNumberType implements ColumnAggregateNumberType<Double> {
    private Double value = 0.0;

    public DoubleNumberType(Double value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        this.value = value;
    }

    public DoubleNumberType() {}

    @Override
    public Double reset() {
        return value = 0.0;
    }

    @Override
    public Double add(Object a) {
        return value += (double) a;
    }

    @Override
    public Double subtract(Object a) {
        return value -= (double) a;
    }

    @Override
    public Double getAvg(Long count) {
        return value / count.doubleValue();
    }

    @Override
    public Double getValue() {
        return value;
    }

    @Override
    public int compareTo(Double o) {
        return value.compareTo(o);
    }
}

class IntegerNumberType implements ColumnAggregateNumberType<Integer> {

    public IntegerNumberType() {}

    public IntegerNumberType(Integer value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        this.value = value;
    }

    private Integer value = 0;

    @Override
    public Integer reset() {
        return value = 0;
    }

    @Override
    public Integer add(Object a) {
        return value += (int) a;
    }

    @Override
    public Integer subtract(Object a) {
        return value -= (int) a;
    }

    @Override
    public Double getAvg(Long count) {
        return value.doubleValue() / count.doubleValue();
    }

    @Override
    public Integer getValue() {
        return value;
    }

    @Override
    public int compareTo(Integer o) {
        return value.compareTo(o);
    }
}

class LongNumberType implements ColumnAggregateNumberType<Long> {
    private Long value = 0L;

    public LongNumberType() {}

    public LongNumberType(Long value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        this.value = value;
    }

    @Override
    public Long reset() {
        return value = 0L;
    }

    @Override
    public Long add(Object a) {
        return value += (long) a;
    }

    @Override
    public Long subtract(Object a) {
        return value -= (long) a;
    }

    @Override
    public Double getAvg(Long count) {
        return value.doubleValue() / count.doubleValue();
    }

    @Override
    public Long getValue() {
        return value;
    }

    @Override
    public int compareTo(Long o) {
        return value.compareTo(o);
    }
}

class FloatNumberType implements ColumnAggregateNumberType<Float> {

    public FloatNumberType() {}

    public FloatNumberType(Float value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        this.value = value;
    }

    private Float value = 0.0f;

    @Override
    public Float reset() {
        return value = 0.0f;
    }

    @Override
    public Float add(Object a) {
        return value += (float) a;
    }

    @Override
    public Float subtract(Object a) {
        return value -= (float) a;
    }

    @Override
    public Double getAvg(Long count) {
        return value.doubleValue() / count.doubleValue();
    }

    @Override
    public Float getValue() {
        return value;
    }

    @Override
    public int compareTo(Float o) {
        return value.compareTo(o);
    }
}

class DecimalDataNumberType implements ColumnAggregateNumberType<DecimalData> {
    private DecimalData value;

    public DecimalDataNumberType(DecimalData value) {
        // maybe is null
        this.value = value;
    }

    @Override
    public DecimalData reset() {
        if (value == null) {
            return null;
        }
        return value = null;
    }

    @Override
    public DecimalData add(Object a) {
        if (value == null && a == null) {
            throw new IllegalArgumentException("Value and args cannot be null");
        }
        if (value == null) {
            value =
                    DecimalData.fromBigDecimal(
                            ((DecimalData) a).toBigDecimal(),
                            ((DecimalData) a).precision(),
                            ((DecimalData) a).scale());
        } else {
            value = DecimalDataUtils.add(value, (DecimalData) a, value.precision(), value.scale());
        }
        return value;
    }

    @Override
    public DecimalData subtract(Object a) {
        if (value == null && a == null) {
            throw new IllegalArgumentException("Value and args cannot be null");
        }
        if (value == null) {
            this.value = DecimalData.zero(((DecimalData) a).precision(), ((DecimalData) a).scale());
            this.value =
                    DecimalDataUtils.subtract(
                            value,
                            (DecimalData) a,
                            ((DecimalData) a).precision(),
                            ((DecimalData) a).scale());
        } else {
            this.value =
                    DecimalDataUtils.subtract(
                            value, (DecimalData) a, value.precision(), value.scale());
        }
        return value;
    }

    @Override
    public Double getAvg(Long count) {
        if (value == null) {
            return null;
        }
        return DecimalDataUtils.doubleValue(value) / count.doubleValue();
    }

    @Override
    public DecimalData getValue() {
        return value;
    }

    @Override
    public int compareTo(DecimalData o) {
        return DecimalDataUtils.compare(value, o);
    }
}

class ByteNumberType implements ColumnAggregateNumberType<Byte> {

    private Byte value = 0;

    public ByteNumberType() {}

    public ByteNumberType(Byte value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        this.value = value;
    }

    @Override
    public Byte reset() {
        return value = 0;
    }

    @Override
    public Byte add(Object a) {
        return value = (byte) (value + (byte) a);
    }

    @Override
    public Byte subtract(Object a) {
        return value = (byte) (value - (byte) a);
    }

    @Override
    public Double getAvg(Long count) {
        return value.doubleValue() / count.doubleValue();
    }

    @Override
    public Byte getValue() {
        return value;
    }

    @Override
    public int compareTo(Byte o) {
        return value.compareTo(o);
    }
}

class ShortNumberType implements ColumnAggregateNumberType<Short> {
    private Short value = 0;

    public ShortNumberType() {}

    public ShortNumberType(Short value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        this.value = value;
    }

    @Override
    public Short reset() {
        return value = 0;
    }

    @Override
    public Short add(Object a) {
        return value = (short) (value + (short) a);
    }

    @Override
    public Short subtract(Object a) {
        return value = (short) (value - (short) a);
    }

    @Override
    public Double getAvg(Long count) {
        return value.doubleValue() / count.doubleValue();
    }

    @Override
    public Short getValue() {
        return value;
    }

    @Override
    public int compareTo(Short o) {
        return value.compareTo(o);
    }
}
