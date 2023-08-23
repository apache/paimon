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

import org.apache.paimon.flink.action.SortCompactAction;
import org.apache.paimon.sort.zorder.ZIndexer;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import java.util.List;

/**
 * This is a table sorter which will sort the records by the z-order of specified columns. It works
 * on stream api. It computes the z-order-index by {@link ZIndexer}. After add the column of
 * z-order, it does the range shuffle and sort. Finally, {@link SortCompactAction} will remove the
 * "z-order" column and insert sorted record to overwrite the origin table.
 */
public class ZorderSorter extends TableSorter {

    public ZorderSorter(
            StreamExecutionEnvironment batchTEnv,
            DataStream<RowData> origin,
            FileStoreTable table,
            List<String> zOrderColNames) {
        super(batchTEnv, origin, table, zOrderColNames);
    }

    @Override
    public DataStream<RowData> sort() {
        return sortStreamByZOrder(origin, table);
    }

    /**
     * Sort the input stream by the given order columns with z-order.
     *
     * @param inputStream the stream waited to be sorted
     * @return the sorted data stream
     */
    private DataStream<RowData> sortStreamByZOrder(
            DataStream<RowData> inputStream, FileStoreTable table) {
        ZIndexer zIndexer = new ZIndexer(table.rowType(), orderColNames);
        return ZorderSorterUtils.sortStreamByZorder(inputStream, zIndexer, table);
    }
}
