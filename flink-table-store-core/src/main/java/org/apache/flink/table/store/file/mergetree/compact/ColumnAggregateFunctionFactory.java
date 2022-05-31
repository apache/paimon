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

import org.apache.flink.table.types.logical.LogicalType;

/** Factory for creating {@link ColumnAggregateFunction}s. */
public class ColumnAggregateFunctionFactory {
    /**
     * Determine the column aggregation function .
     *
     * @param kind the kind of aggregation
     * @param typeAt the type of the column
     * @return the column aggregation function
     */
    public static ColumnAggregateFunction<?, ?> getColumnAggregateFunction(
            AggregationKind kind, LogicalType type) {
        switch (kind) {
            case SUM:
                return new SumColumnAggregateFunction<>(getNumberType(type));
            case AVG:
                return new AvgColumnAggregateFunction<>(getNumberType(type));
            case MAX:
                return new MaxColumnAggregateFunction<>(getNumberType(type));
            case MIN:
                return new MinColumnAggregateFunction<>(getNumberType(type));
            default:
                throw new IllegalArgumentException("Aggregation kind " + kind + " not supported");
        }
    }

    /**
     * Determine the column data types that can be aggregated.
     *
     * @param type the row type of the column
     * @return the column data types that can be aggregated
     */
    static ColumnAggregateNumberType<?> getNumberType(LogicalType type) {
        switch (type.getTypeRoot()) {
            case INTEGER:
                return new IntegerNumberType();
            case BIGINT:
                return new LongNumberType();
            case FLOAT:
                return new FloatNumberType();
            case DOUBLE:
                return new DoubleNumberType();
            case DECIMAL:
                return new DecimalDataNumberType(null);
            case TINYINT:
                return new ByteNumberType();
            case SMALLINT:
                return new ShortNumberType();
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            default:
                throw new UnsupportedOperationException("Unsupported type " + type.toString());
        }
    }
}
