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

package org.apache.paimon.globalindex.sorted;

import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** A bounded reader-local buffer for transporting null row IDs through an index shuffle. */
public final class NullBitmapBuffer {

    public static final int MAX_ROWS = 4096;

    private final RoaringNavigableMap64 rows = new RoaringNavigableMap64();
    private int count;
    private long firstRowId = Long.MAX_VALUE;

    public void add(long rowId) {
        checkArgument(rowId >= 0, "Row ID must be non-negative.");
        checkState(!isFull(), "Null bitmap buffer is full.");
        rows.add(rowId);
        firstRowId = Math.min(firstRowId, rowId);
        count++;
    }

    public boolean isFull() {
        return count == MAX_ROWS;
    }

    public boolean isEmpty() {
        return count == 0;
    }

    public long firstRowId() {
        checkState(!isEmpty(), "Null bitmap buffer is empty.");
        return firstRowId;
    }

    public byte[] drain() throws IOException {
        checkState(!isEmpty(), "Null bitmap buffer is empty.");
        byte[] bytes = rows.serialize();
        rows.clear();
        count = 0;
        firstRowId = Long.MAX_VALUE;
        return bytes;
    }
}
