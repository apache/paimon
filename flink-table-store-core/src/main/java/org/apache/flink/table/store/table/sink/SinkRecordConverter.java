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
import org.apache.flink.table.store.codegen.CodeGenUtils;
import org.apache.flink.table.store.codegen.Projection;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;

/** Converter for converting {@link RowData} to {@link SinkRecord}. */
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

    public SinkRecord convert(RowData row) {
        BinaryRowData partition = partProjection.apply(row);
        BinaryRowData primaryKey = primaryKey(row);
        int bucket = bucketComputer.bucket(row, primaryKey);
        return new SinkRecord(partition, bucket, primaryKey, row);
    }

    public SinkRecord convertToLogSinkRecord(SinkRecord record) {
        if (logPkProjection == null) {
            return record;
        }
        BinaryRowData logPrimaryKey = logPrimaryKey(record.row());
        return new SinkRecord(record.partition(), record.bucket(), logPrimaryKey, record.row());
    }

    public int bucket(RowData row) {
        return bucketComputer.bucket(row);
    }

    private BinaryRowData primaryKey(RowData row) {
        return pkProjection.apply(row);
    }

    private BinaryRowData logPrimaryKey(RowData row) {
        assert logPkProjection != null;
        return logPkProjection.apply(row);
    }
}
