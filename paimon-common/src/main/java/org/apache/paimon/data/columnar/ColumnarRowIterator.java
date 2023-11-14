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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.RecyclableIterator;

import javax.annotation.Nullable;

/**
 * A {@link RecordReader.RecordIterator} that returns {@link InternalRow}s. The next row is set by
 * {@link ColumnarRow#setRowId}.
 */
public class ColumnarRowIterator extends RecyclableIterator<InternalRow> {

    private final ColumnarRow rowData;
    private boolean mapped = false;

    private int num;
    private int pos;

    public ColumnarRowIterator(ColumnarRow rowData, @Nullable Runnable recycler) {
        super(recycler);
        this.rowData = rowData;
    }

    public void set(int num) {
        this.num = num;
        this.pos = 0;
    }

    @Nullable
    @Override
    public InternalRow next() {
        if (pos < num) {
            rowData.setRowId(pos++);
            return rowData;
        } else {
            return null;
        }
    }

    public ColumnarRowIterator mapping(
            @Nullable PartitionInfo partitionInfo, @Nullable int[] indexMapping) {
        if (!mapped) {
            rowData.mapping(partitionInfo, indexMapping);
            mapped = true;
        }
        return this;
    }
}
