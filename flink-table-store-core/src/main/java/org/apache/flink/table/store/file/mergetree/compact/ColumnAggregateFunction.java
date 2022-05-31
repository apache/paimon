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

import java.io.Serializable;

/** Custom column aggregation interface. */
public class ColumnAggregateFunction<T, R> implements Serializable {
    protected ColumnAggregateNumberType<T> numberType;
    protected Long count = 0L;

    public ColumnAggregateFunction(ColumnAggregateNumberType<T> numberType) {
        if (numberType == null) {
            throw new IllegalArgumentException("NumberType cannot be null");
        }
        this.numberType = numberType;
        this.count = 0L;
    }

    public R getResult() {
        return (R) numberType.getValue();
    }

    public void reset() {
        numberType.reset();
        count = 0L;
    }

    public void aggregate(Object value) {
        if (value != null) {
            numberType.add(value);
            count++;
        }
    }

    public Long getCount() {
        return count;
    }
}

/** Sum column aggregation interface. */
class SumColumnAggregateFunction<T> extends ColumnAggregateFunction<T, T> {
    public SumColumnAggregateFunction(ColumnAggregateNumberType<T> numberType) {
        super(numberType);
    }
}

/** Avg column aggregation interface. */
class AvgColumnAggregateFunction<T> extends ColumnAggregateFunction<T, Double> {
    public AvgColumnAggregateFunction(ColumnAggregateNumberType<T> numberType) {
        super(numberType);
    }

    @Override
    public Double getResult() {
        return numberType.getAvg(count);
    }
}

/** Max column aggregation interface. */
class MaxColumnAggregateFunction<T> extends ColumnAggregateFunction<T, T> {

    @Override
    public void aggregate(Object value) {
        if (value != null) {
            if (numberType.compareTo((T) value) < 0) {
                numberType.reset();
                numberType.add(value);
            }
            count++;
        }
    }

    public MaxColumnAggregateFunction(ColumnAggregateNumberType<T> numberType) {
        super(numberType);
    }
}

/** Min column aggregation interface. */
class MinColumnAggregateFunction<T> extends ColumnAggregateFunction<T, T> {

    @Override
    public void aggregate(Object value) {
        if (value != null) {
            if (numberType.compareTo((T) value) > 0) {
                numberType.reset();
                numberType.add(value);
            }
            count++;
        }
    }

    public MinColumnAggregateFunction(ColumnAggregateNumberType<T> numberType) {
        super(numberType);
    }
}
