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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Reusable binary view of the projected fields needed by index manifest scans. */
public final class BinaryIndexManifestEntry {

    private @Nullable InternalRow row;

    BinaryIndexManifestEntry replace(InternalRow row) {
        checkArgument(row != null, "Index manifest row cannot be null.");
        this.row = row;
        return this;
    }

    void clear() {
        row = null;
    }

    public boolean isAdd() {
        return current().getByte(1) == FileKind.ADD.toByteValue();
    }

    public boolean isDelete() {
        return current().getByte(1) == FileKind.DELETE.toByteValue();
    }

    public byte[] partitionBytes() {
        byte[] partition = current().getBinary(2);
        checkState(partition != null, "Serialized index manifest partition cannot be null.");
        return partition;
    }

    public int bucket() {
        return current().getInt(3);
    }

    public BinaryString indexType() {
        BinaryString indexType = current().getString(4);
        checkState(indexType != null, "Index type cannot be null.");
        return indexType;
    }

    public boolean hasGlobalIndexMeta() {
        return !current().isNullAt(5);
    }

    public long rowRangeStart() {
        return globalIndex().getLong(0);
    }

    public long rowRangeEnd() {
        return globalIndex().getLong(1);
    }

    public int indexFieldId() {
        return globalIndex().getInt(2);
    }

    public boolean hasExtraFields() {
        InternalRow global = globalIndex();
        return !global.isNullAt(3) && global.getArray(3).size() > 0;
    }

    private InternalRow globalIndex() {
        InternalRow global = current().getRow(5, 4);
        checkState(global != null, "Global index metadata is not present.");
        return global;
    }

    private InternalRow current() {
        checkState(row != null, "Binary index manifest entry is not backed by a row.");
        return row;
    }
}
