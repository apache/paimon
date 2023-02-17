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

import org.apache.flink.table.store.codegen.CodeGenUtils;
import org.apache.flink.table.store.codegen.Projection;
import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.types.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;

/** Converter for converting {@link InternalRow} to {@link SinkRecord}. */
public class SinkRecordConverter {

    private final BucketComputer bucketComputer;

    private final Projection partProjection;

    private final Projection pkProjection;

    @Nullable private final Projection logPkProjection;

    public SinkRecordConverter(TableSchema tableSchema) {
        this(
                tableSchema.logicalRowType(),
                tableSchema.projection(tableSchema.partitionKeys()),
                tableSchema.projection(tableSchema.trimmedPrimaryKeys()),
                tableSchema.projection(tableSchema.primaryKeys()),
                new BucketComputer(tableSchema));
    }

    private SinkRecordConverter(
            RowType inputType,
            int[] partitions,
            int[] primaryKeys,
            int[] logPrimaryKeys,
            BucketComputer bucketComputer) {
        this.bucketComputer = bucketComputer;
        this.partProjection = CodeGenUtils.newProjection(inputType, partitions);
        this.pkProjection = CodeGenUtils.newProjection(inputType, primaryKeys);
        this.logPkProjection =
                Arrays.equals(primaryKeys, logPrimaryKeys)
                        ? null
                        : CodeGenUtils.newProjection(inputType, logPrimaryKeys);
    }

    public SinkRecord convert(InternalRow row) {
        BinaryRow partition = partProjection.apply(row);
        BinaryRow primaryKey = primaryKey(row);
        int bucket = bucketComputer.bucket(row, primaryKey);
        return new SinkRecord(partition, bucket, primaryKey, row);
    }

    public SinkRecord convertToLogSinkRecord(SinkRecord record) {
        if (logPkProjection == null) {
            return record;
        }
        BinaryRow logPrimaryKey = logPrimaryKey(record.row());
        return new SinkRecord(record.partition(), record.bucket(), logPrimaryKey, record.row());
    }

    public BinaryRow partition(InternalRow row) {
        return partProjection.apply(row).copy();
    }

    public int bucket(InternalRow row) {
        return bucketComputer.bucket(row);
    }

    private BinaryRow primaryKey(InternalRow row) {
        return pkProjection.apply(row);
    }

    private BinaryRow logPrimaryKey(InternalRow row) {
        assert logPkProjection != null;
        return logPkProjection.apply(row);
    }
}
