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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Aggregate Function Factory is used to get the aggregate type based on the configuration Each
 * aggregate type has its own aggregate function factory Different implementation classes are given
 * for different data types.
 */
public class AggregateFunctionFactory {

    /** SumFactory. */
    public static class SumAggregateFunctionFactory {
        static SumAggregateFunction<?> choiceRightAggregateFunction(Class<?> c) {
            SumAggregateFunction<?> f = null;
            if (Double.class.equals(c)) {
                f = new DoubleSumAggregateFunction();
            } else if (Long.class.equals(c)) {
                f = new LongSumAggregateFunction();
            } else if (Integer.class.equals(c)) {
                f = new IntegerSumAggregateFunction();
            } else if (Float.class.equals(c)) {
                f = new FloatSumAggregateFunction();
            }
            return f;
        }
    }

    public static AggregationKind getAggregationKind(Collection<String> values) {
        Set<String> set = new HashSet<>(values);
        if (set.size() != 1) {
            throw new IllegalArgumentException(
                    "Aggregate function must be the same for all columns");
        }
        for (String e : set) {
            if (e.equals("sum")) {
                return AggregationKind.Sum;
            }
            if (e.equals("avg")) {
                return AggregationKind.Avg;
            }
            if (e.equals("max")) {
                return AggregationKind.Max;
            }
            if (e.equals("min")) {
                return AggregationKind.Min;
            }
        }
        throw new IllegalArgumentException("Aggregate function must be the same for all columns");
    }
}
