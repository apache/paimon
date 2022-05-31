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
    public static ColumnAggregateFunction<?> getColumnAggregateFunction(
            AggregationKind kind, LogicalType typeAt) {
        switch (kind) {
            case SUM:
                return SumColumnAggregateFunctionFactory.getColumnAggregateFunction(typeAt);
            case AVG:
                return AvgColumnAggregateFunctionFactory.getColumnAggregateFunction(typeAt);
            case MAX:
                return MaxColumnAggregateFunctionFactory.getColumnAggregateFunction(typeAt);
            case MIN:
                return MinColumnAggregateFunctionFactory.getColumnAggregateFunction(typeAt);
            default:
                throw new IllegalArgumentException("Aggregation kind " + kind + " not supported");
        }
    }

    /** AggregateKind is Sum. Determine the column aggregation function . */
    private static class SumColumnAggregateFunctionFactory {
        static SumColumnAggregateFunction<?> getColumnAggregateFunction(LogicalType type) {
            switch (type.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                case BOOLEAN:
                case BINARY:
                case VARBINARY:
                case DECIMAL:
                case TINYINT:
                case SMALLINT:
                default:
                    throw new UnsupportedOperationException("Unsupported type " + type.toString());
                case INTEGER:
                    return new IntegerSumColumnAggregateFunction();
                case BIGINT:
                    return new LongSumColumnAggregateFunction();
                case FLOAT:
                    return new FloatSumColumnAggregateFunction();
                case DOUBLE:
                    return new DoubleSumColumnAggregateFunction();
            }
        }
    }

    /** AggregateKind is Max. Determine the column aggregation function . */
    private static class MaxColumnAggregateFunctionFactory {
        static MaxColumnAggregateFunction<?> getColumnAggregateFunction(LogicalType type) {
            switch (type.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                case BOOLEAN:
                case BINARY:
                case VARBINARY:
                case DECIMAL:
                case TINYINT:
                case SMALLINT:
                default:
                    throw new UnsupportedOperationException("Unsupported type " + type.toString());
                case INTEGER:
                    return new IntegerMaxColumnAggregateFunction();
                case BIGINT:
                    return new LongMaxColumnAggregateFunction();
                case FLOAT:
                    return new FloatMaxColumnAggregateFunction();
                case DOUBLE:
                    return new DoubleMaxColumnAggregateFunction();
            }
        }
    }

    /** AggregateKind is Min. Determine the column aggregation function . */
    private static class MinColumnAggregateFunctionFactory {
        static MinColumnAggregateFunction<?> getColumnAggregateFunction(LogicalType type) {
            switch (type.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                case BOOLEAN:
                case BINARY:
                case VARBINARY:
                case DECIMAL:
                case TINYINT:
                case SMALLINT:
                default:
                    throw new UnsupportedOperationException("Unsupported type " + type.toString());
                case INTEGER:
                    return new IntegerMinColumnAggregateFunction();
                case BIGINT:
                    return new LongMinColumnAggregateFunction();
                case FLOAT:
                    return new FloatMinColumnAggregateFunction();
                case DOUBLE:
                    return new DoubleMinColumnAggregateFunction();
            }
        }
    }

    /** AggregateKind is Avg. Determine the column aggregation function . */
    private static class AvgColumnAggregateFunctionFactory {
        static AvgColumnAggregateFunction<?> getColumnAggregateFunction(LogicalType type) {
            switch (type.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                case BOOLEAN:
                case BINARY:
                case VARBINARY:
                case DECIMAL:
                case TINYINT:
                case SMALLINT:
                default:
                    throw new UnsupportedOperationException("Unsupported type " + type.toString());
                case INTEGER:
                    return new IntegerAvgColumnAggregateFunction();
                case BIGINT:
                    return new LongAvgColumnAggregateFunction();
                case FLOAT:
                    return new FloatAvgColumnAggregateFunction();
                case DOUBLE:
                    return new DoubleAvgColumnAggregateFunction();
            }
        }
    }
}
