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
public interface ColumnAggregateFunction<T> extends Serializable {
    T getResult();

    void reset();

    void aggregate(Object value);

    void reset(Object value);
}

/** Sum column aggregation interface. */
interface SumColumnAggregateFunction<T> extends ColumnAggregateFunction<T> {
    void retract(Object value);
}

class DoubleSumColumnAggregateFunction implements SumColumnAggregateFunction<Double> {
    Double aggregator;

    @Override
    public Double getResult() {
        return aggregator;
    }

    @Override
    public void reset() {
        aggregator = 0.0;
    }

    @Override
    public void aggregate(Object value) {
        aggregator += (Double) value;
    }

    @Override
    public void retract(Object value) {
        aggregator -= (Double) value;
    }

    @Override
    public void reset(Object value) {
        aggregator = (Double) value;
    }
}

class LongSumColumnAggregateFunction implements SumColumnAggregateFunction<Long> {

    Long aggregator;

    @Override
    public Long getResult() {
        return aggregator;
    }

    @Override
    public void reset() {
        aggregator = 0L;
    }

    @Override
    public void aggregate(Object value) {
        aggregator += (Long) value;
    }

    @Override
    public void retract(Object value) {
        aggregator -= (Long) value;
    }

    @Override
    public void reset(Object value) {
        aggregator = (Long) value;
    }
}

class IntegerSumColumnAggregateFunction implements SumColumnAggregateFunction<Integer> {
    Integer aggregator;

    @Override
    public Integer getResult() {
        return aggregator;
    }

    @Override
    public void reset() {
        aggregator = 0;
    }

    @Override
    public void aggregate(Object value) {
        aggregator += (Integer) value;
    }

    @Override
    public void retract(Object value) {
        aggregator -= (Integer) value;
    }

    @Override
    public void reset(Object value) {
        aggregator = (Integer) value;
    }
}

class FloatSumColumnAggregateFunction implements SumColumnAggregateFunction<Float> {
    Float aggregator;

    @Override
    public Float getResult() {
        return aggregator;
    }

    @Override
    public void reset() {
        aggregator = 0.0f;
    }

    @Override
    public void aggregate(Object value) {
        aggregator += (Float) value;
    }

    @Override
    public void retract(Object value) {
        aggregator -= (Float) value;
    }

    @Override
    public void reset(Object value) {
        aggregator = (Float) value;
    }
}
