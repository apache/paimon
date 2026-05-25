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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;

/**
 * A tagged record carrying classification metadata through the network shuffle between {@link
 * UpsertClassifyOperator} (Phase 1) and {@link UpsertWriteOperator} (Phase 2).
 */
public class UpsertRecord {

    private final BinaryRow partition;
    private final long firstRowId;
    private final long offset;
    private final InternalRow row;

    public UpsertRecord(BinaryRow partition, long firstRowId, long offset, InternalRow row) {
        this.partition = partition;
        this.firstRowId = firstRowId;
        this.offset = offset;
        this.row = row;
    }

    public BinaryRow partition() {
        return partition;
    }

    public long firstRowId() {
        return firstRowId;
    }

    public long offset() {
        return offset;
    }

    public InternalRow row() {
        return row;
    }

    public boolean isInsert() {
        return firstRowId < 0;
    }
}
