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

package org.apache.flink.table.store.table.sink;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.store.codegen.CodeGenUtils;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.stream.IntStream;

/** Converter for converting {@link RowData} to {@link SinkRecord}. */
public class SinkRecordConverter {

    private final int numBucket;

    private final Projection<RowData, BinaryRowData> allProjection;

    private final Projection<RowData, BinaryRowData> partProjection;

    private final Projection<RowData, BinaryRowData> bucketProjection;

    private final Projection<RowData, BinaryRowData> pkProjection;

    @Nullable private final Projection<RowData, BinaryRowData> logPkProjection;

    public SinkRecordConverter(int numBucket, TableSchema tableSchema) {
        this(
                numBucket,
                tableSchema.logicalRowType(),
                tableSchema.projection(tableSchema.partitionKeys()),
                tableSchema.projection(tableSchema.originalBucketKeys()),
                tableSchema.projection(tableSchema.trimmedPrimaryKeys()),
                tableSchema.projection(tableSchema.primaryKeys()));
    }

    private SinkRecordConverter(
            int numBucket,
            RowType inputType,
            int[] partitions,
            int[] bucketKeys,
            int[] primaryKeys,
            int[] logPrimaryKeys) {
        this.numBucket = numBucket;
        this.allProjection =
                CodeGenUtils.newProjection(
                        inputType, IntStream.range(0, inputType.getFieldCount()).toArray());
        this.partProjection = CodeGenUtils.newProjection(inputType, partitions);
        this.bucketProjection = CodeGenUtils.newProjection(inputType, bucketKeys);
        this.pkProjection = CodeGenUtils.newProjection(inputType, primaryKeys);
        this.logPkProjection =
                Arrays.equals(primaryKeys, logPrimaryKeys)
                        ? null
                        : CodeGenUtils.newProjection(inputType, logPrimaryKeys);
    }

    public SinkRecord convert(RowData row) {
        BinaryRowData partition = partProjection.apply(row);
        int bucket = bucket(row, bucketKey(row, primaryKey(row)));
        return new SinkRecord(partition, bucket, row);
    }

    public SinkRecord convertToLogSinkRecord(SinkRecord record) {
        if (logPkProjection == null) {
            return record;
        }
        BinaryRowData logPrimaryKey = logPrimaryKey(record.row());
        return new SinkRecord(record.partition(), record.bucket(), logPrimaryKey, record.row());
    }

    public int bucket(RowData row) {
        return bucket(row, bucketKey(row));
    }

    private BinaryRowData primaryKey(RowData row) {
        return pkProjection.apply(row);
    }

    private BinaryRowData bucketKey(RowData row) {
        BinaryRowData bucketKey = bucketProjection.apply(row);
        return bucketKey.getArity() == 0 ? pkProjection.apply(row) : bucketKey;
    }

    private BinaryRowData bucketKey(RowData row, BinaryRowData primaryKey) {
        BinaryRowData bucketKey = bucketProjection.apply(row);
        return bucketKey.getArity() == 0 ? primaryKey : bucketKey;
    }

    private BinaryRowData logPrimaryKey(RowData row) {
        assert logPkProjection != null;
        return logPkProjection.apply(row);
    }

    private int bucket(RowData row, BinaryRowData bucketKey) {
        int hash = bucketKey.getArity() == 0 ? hashRow(row) : bucketKey.hashCode();
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
