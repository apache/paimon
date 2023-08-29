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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.shuffle.RangeShuffle;
import org.apache.paimon.sort.zorder.ZIndexer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyProjectedRow;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.guava30.com.google.common.primitives.UnsignedBytes;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

/**
 * This is a table sorter which will sort the records by the z-order of the z-indexer generates. It
 * is a global sort method, we will shuffle the input stream through z-order. After sorted, we
 * convert datastream from paimon RowData back to Flink RowData
 *
 * <pre>
 *                         toPaimonDataStream                        add z-order column                             range shuffle by z-order                                 local sort                                              remove z-index
 * DataStream[RowData] -------------------> DataStream[PaimonRowData] -------------------> DataStream[PaimonRowData] -------------------------> DataStream[PaimonRowData] -----------------------> DataStream[PaimonRowData sorted] ----------------------------> DataStream[RowData sorted]
 *                                                                                                                                                                                                                                back to flink RowData
 * </pre>
 */
public class ZorderSorterUtils {

    private static final RowType KEY_TYPE =
            new RowType(Collections.singletonList(new DataField(0, "Z_INDEX", DataTypes.BYTES())));

    /**
     * Sort the input stream by z-order.
     *
     * @param inputStream the stream wait to be ordered
     * @param zIndexer generate z-index by the given row
     * @param table the FileStoreTable
     */
    public static DataStream<RowData> sortStreamByZorder(
            DataStream<RowData> inputStream, ZIndexer zIndexer, FileStoreTable table) {

        final RowType valueRowType = table.rowType();
        final int fieldCount = valueRowType.getFieldCount();
        final int parallelism = inputStream.getParallelism();
        final int sampleSize = parallelism * 1000;
        final int rangeNum = parallelism * 10;
        final long maxSortMemory = table.coreOptions().writeBufferSize();
        final int pageSize = table.coreOptions().pageSize();

        // generate the z-index as the key of Pair.
        DataStream<Pair<byte[], RowData>> inputWithKey =
                inputStream
                        .map(
                                new RichMapFunction<RowData, Pair<byte[], RowData>>() {

                                    @Override
                                    public void open(Configuration parameters) throws Exception {
                                        super.open(parameters);
                                        zIndexer.open();
                                    }

                                    @Override
                                    public Pair<byte[], RowData> map(RowData value) {
                                        byte[] zorder = zIndexer.index(new FlinkRowWrapper(value));
                                        return Pair.of(Arrays.copyOf(zorder, zorder.length), value);
                                    }
                                })
                        .setParallelism(parallelism);

        // range shuffle by z-index key
        return RangeShuffle.rangeShuffleByKey(
                        inputWithKey,
                        (Comparator<byte[]> & Serializable)
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
                        byte[].class,
                        sampleSize,
                        rangeNum)
                .map(
                        a ->
                                new JoinedRow(
                                        GenericRow.of(a.getLeft()),
                                        new FlinkRowWrapper(a.getRight())),
                        TypeInformation.of(InternalRow.class))
                .setParallelism(parallelism)
                // sort the output locally by `SortOperator`
                .transform(
                        "LOCAL SORT",
                        TypeInformation.of(InternalRow.class),
                        new SortOperator(KEY_TYPE, valueRowType, maxSortMemory, pageSize))
                .setParallelism(parallelism)
                // remove the z-index column from every row
                .map(
                        new RichMapFunction<InternalRow, InternalRow>() {

                            private transient KeyProjectedRow keyProjectedRow;

                            @Override
                            public void open(Configuration parameters) {
                                int[] map = new int[fieldCount];
                                for (int i = 0; i < map.length; i++) {
                                    map[i] = i + 1;
                                }
                                keyProjectedRow = new KeyProjectedRow(map);
                            }

                            @Override
                            public InternalRow map(InternalRow value) {
                                return keyProjectedRow.replaceRow(value);
                            }
                        })
                .setParallelism(parallelism)
                .map(FlinkRowData::new, inputStream.getType())
                .setParallelism(parallelism);
    }
}
