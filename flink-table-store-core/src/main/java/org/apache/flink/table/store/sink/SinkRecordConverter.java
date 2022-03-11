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

package org.apache.flink.table.store.sink;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.store.utils.ProjectionUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.stream.IntStream;

/** Converter for converting {@link RowData} to {@link SinkRecord}. */
public class SinkRecordConverter {

    private final int numBucket;

    private final Projection<RowData, BinaryRowData> allProjection;

    private final Projection<RowData, BinaryRowData> partProjection;

    private final Projection<RowData, BinaryRowData> keyProjection;

    public SinkRecordConverter(int numBucket, RowType inputType, int[] partitions, int[] keys) {
        this.numBucket = numBucket;
        this.allProjection =
                ProjectionUtils.newProjection(
                        inputType, IntStream.range(0, inputType.getFieldCount()).toArray());
        this.partProjection = ProjectionUtils.newProjection(inputType, partitions);
        this.keyProjection = ProjectionUtils.newProjection(inputType, keys);
    }

    public SinkRecord convert(RowData row) {
        BinaryRowData partition = partProjection.apply(row);
        BinaryRowData key = key(row);
        int bucket = bucket(row, key);
        return new SinkRecord(partition, bucket, key, row);
    }

    public BinaryRowData key(RowData row) {
        return keyProjection.apply(row);
    }

    public int bucket(RowData row, BinaryRowData key) {
        int hash = key.getArity() == 0 ? hashRow(row) : key.hashCode();
        return Math.abs(hash % numBucket);
    }

    private int hashRow(RowData row) {
        if (row instanceof BinaryRowData) {
            RowKind rowKind = row.getRowKind();
            row.setRowKind(RowKind.INSERT);
            int hash = row.hashCode();
            row.setRowKind(rowKind);
            return hash;
        } else {
            return allProjection.apply(row).hashCode();
        }
    }
}
