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

import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.action.OrderRewriteAction;
import org.apache.paimon.sort.zorder.ZIndexer;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/**
 * This is a table sorter which will sort the records by the z-order of specified columns. It works
 * in a way mixed by table api and stream api. By sql select, it constructs the input stream, then
 * compute the z-order-index using stream api. After add the column of z-order, it transfers the
 * data stream back to table, and use the sql clause "order by" to sort. Finally, {@link
 * OrderRewriteAction} will remove the "z-order" column and insert sorted record to the target
 * table.
 *
 * <pre>
 *                     toDataStream                    add z-order column                   fromDataStream                           order by
 * Table(sql select) ----------------> DataStream[Row] -------------------> DataStream[Row] -----------------> Table(with z-order) -----------------> Table(sorted)
 * </pre>
 */
public class ZorderSorter extends TableSorter {

    public ZorderSorter(
            StreamTableEnvironment batchTEnv, Table origin, List<String> zOrderColNames) {
        super(batchTEnv, origin, zOrderColNames);
    }

    @Override
    public Table sort() {
        final ResolvedSchema resolvedSchema = origin.getResolvedSchema();
        Schema schema = Schema.newBuilder().fromResolvedSchema(resolvedSchema).build();
        DataType type = resolvedSchema.toSourceRowDataType();
        DataStream<RowData> input =
                batchTEnv.toDataStream(
                        origin,
                        new FieldsDataType(
                                type.getLogicalType(),
                                RowData.class,
                                resolvedSchema.getColumnDataTypes()));
        return batchTEnv.fromDataStream(sortStreamByZOrder(input), schema);
    }

    /**
     * Sort the input stream by the given order columns with z-order.
     *
     * @param inputStream the stream waited to be sorted
     * @return the sorted data stream
     */
    private DataStream<RowData> sortStreamByZOrder(DataStream<RowData> inputStream) {
        org.apache.paimon.types.RowType rowType =
                LogicalTypeConversion.toDataType(
                        (RowType)
                                origin.getResolvedSchema().toSourceRowDataType().getLogicalType());
        ZIndexer zIndexer = new ZIndexer(rowType, orderColNames);
        return ZorderSorterUtils.sortStreamByZorder(inputStream, zIndexer, rowType);
    }
}
