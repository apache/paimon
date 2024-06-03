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

package org.apache.paimon.flink.sorter;

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.Projection;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import static org.apache.paimon.table.PrimaryKeyTableUtils.addKeyNamePrefix;

/** Alphabetical order sorter to sort records by the given `orderColNames`. */
public class OrderSorter extends TableSorter {

    private final TableSortInfo tableSortInfo;

    public OrderSorter(
            StreamExecutionEnvironment batchTEnv,
            DataStream<RowData> origin,
            FileStoreTable table,
            TableSortInfo tableSortInfo) {
        super(batchTEnv, origin, table, tableSortInfo.getSortColumns());
        this.tableSortInfo = tableSortInfo;
    }

    @Override
    public DataStream<RowData> sort() {
        final RowType valueRowType = table.rowType();
        final int[] keyProjectionMap = table.schema().projection(orderColNames);
        final RowType keyRowType =
                addKeyNamePrefix(Projection.of(keyProjectionMap).project(valueRowType));

        return SortUtils.sortStreamByKey(
                origin,
                table,
                keyRowType,
                InternalTypeInfo.fromRowType(keyRowType),
                new KeyComparatorSupplier(keyRowType),
                new SortUtils.KeyAbstract<InternalRow>() {

                    private transient org.apache.paimon.codegen.Projection keyProjection;

                    @Override
                    public void open() {
                        // use key gen to speed up projection
                        keyProjection = CodeGenUtils.newProjection(valueRowType, keyProjectionMap);
                    }

                    @Override
                    public InternalRow apply(RowData value) {
                        // deep copy by wrapper the Flink RowData
                        return keyProjection.apply(new FlinkRowWrapper(value)).copy();
                    }
                },
                row -> row,
                tableSortInfo);
    }
}
