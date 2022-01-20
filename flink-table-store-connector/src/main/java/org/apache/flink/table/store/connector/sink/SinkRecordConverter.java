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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.connector.utils.ProjectionUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/** Converter for converting {@link RowData} to {@link SinkRecord}. */
public class SinkRecordConverter {

    private final int numBucket;

    private final RowDataSerializer rowSerializer;

    private final Projection<RowData, BinaryRowData> partProjection;

    private final Projection<RowData, BinaryRowData> keyProjection;

    public SinkRecordConverter(int numBucket, RowType inputType, int[] partitions, int[] keys) {
        this.numBucket = numBucket;
        this.rowSerializer = new RowDataSerializer(inputType);
        this.partProjection = ProjectionUtils.newProjection(inputType, partitions);
        this.keyProjection = ProjectionUtils.newProjection(inputType, keys);
    }

    public SinkRecord convert(RowData row) {
        RowKind rowKind = row.getRowKind();
        row.setRowKind(RowKind.INSERT);
        BinaryRowData partition = partProjection.apply(row);
        BinaryRowData key = keyProjection.apply(row);
        int hash = key.getArity() == 0 ? rowSerializer.toBinaryRow(row).hashCode() : key.hashCode();
        int bucket = Math.abs(hash % numBucket);
        return new SinkRecord(partition, bucket, rowKind, key, row);
    }
}
