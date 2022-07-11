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
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A sink record contains key, row and partition, bucket information. */
public class SinkRecord {

    private final BinaryRowData partition;

    private final int bucket;

    private final @Nullable BinaryRowData primaryKey;

    private final RowData row;

    public SinkRecord(BinaryRowData partition, int bucket, RowData row) {
        checkArgument(partition.getRowKind() == RowKind.INSERT);
        this.partition = partition;
        this.bucket = bucket;
        this.row = row;
        // Avoid redundant storage for primary key.
        this.primaryKey = null;
    }

    public SinkRecord(BinaryRowData partition, int bucket, BinaryRowData primaryKey, RowData row) {
        checkArgument(partition.getRowKind() == RowKind.INSERT);
        checkArgument(primaryKey.getRowKind() == RowKind.INSERT);
        this.partition = partition;
        this.bucket = bucket;
        this.primaryKey = primaryKey;
        this.row = row;
    }

    public BinaryRowData partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public BinaryRowData primaryKey() {
        return primaryKey;
    }

    public RowData row() {
        return row;
    }
}
