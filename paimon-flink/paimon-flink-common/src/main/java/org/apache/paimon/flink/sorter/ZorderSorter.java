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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.action.SortCompactAction;
import org.apache.paimon.sort.zorder.ZIndexer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.primitives.UnsignedBytes;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import java.util.Arrays;
import java.util.Collections;

/**
 * This is a table sorter which will sort the records by the z-order of specified columns. It works
 * on stream api. It computes the z-order-index by {@link ZIndexer}. After add the column of
 * z-order, it does the range shuffle and sort. Finally, {@link SortCompactAction} will remove the
 * "z-order" column and insert sorted record to overwrite the origin table.
 */
public class ZorderSorter extends TableSorter {

    private static final RowType KEY_TYPE =
            new RowType(Collections.singletonList(new DataField(0, "Z_INDEX", DataTypes.BYTES())));

    private final TableSortInfo sortInfo;

    public ZorderSorter(
            StreamExecutionEnvironment batchTEnv,
            DataStream<RowData> origin,
            FileStoreTable table,
            TableSortInfo sortInfo) {
        super(batchTEnv, origin, table, sortInfo.getSortColumns());
        this.sortInfo = sortInfo;
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
        final ZIndexer zIndexer =
                new ZIndexer(table.rowType(), orderColNames, table.coreOptions().varTypeSize());
        return SortUtils.sortStreamByKey(
                inputStream,
                table,
                KEY_TYPE,
                TypeInformation.of(byte[].class),
                () ->
                        (b1, b2) -> {
                            assert b1.length == b2.length;
                            for (int i = 0; i < b1.length; i++) {
                                int ret = UnsignedBytes.compare(b1[i], b2[i]);
                                if (ret != 0) {
                                    return ret;
                                }
                            }
                            return 0;
                        },
                new SortUtils.KeyAbstract<byte[]>() {
                    @Override
                    public void open() {
                        zIndexer.open();
                    }

                    @Override
                    public byte[] apply(RowData value) {
                        byte[] zorder = zIndexer.index(new FlinkRowWrapper(value));
                        // we can just return the reused bytes zorder, because the sample operator
                        // will remember the record to sample.
                        return Arrays.copyOf(zorder, zorder.length);
                    }
                },
                GenericRow::of,
                sortInfo);
    }
}
