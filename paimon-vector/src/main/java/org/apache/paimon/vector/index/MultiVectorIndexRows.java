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

package org.apache.paimon.vector.index;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.LongArrayList;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/** Versioned prefix of a multivector shard. Child IDs are dense in insertion order. */
final class MultiVectorIndexRows {
    private static final int MAGIC = 0x504d5631; // PMV1
    private final LongArrayList rowIds = new LongArrayList(16);
    private final LongArrayList ends = new LongArrayList(16);
    private int dimension;

    void append(long parent, long end) {
        rowIds.add(parent);
        ends.add(end);
    }

    long sizeInBytes() {
        return 12L + 16L * ends.size();
    }

    long childCount() {
        return ends.isEmpty() ? 0 : ends.get(ends.size() - 1);
    }

    int dimension() {
        return dimension;
    }

    long parent(long child) {
        return rowIds.get(groupIndex(child));
    }

    Range siblings(long child) {
        int index = groupIndex(child);
        return new Range(index == 0 ? 0 : ends.get(index - 1), ends.get(index) - 1);
    }

    private int groupIndex(long child) {
        if (child < 0 || child >= childCount()) {
            throw new IllegalArgumentException("Child ID outside multivector shard: " + child);
        }
        int low = 0;
        int high = ends.size() - 1;
        while (low < high) {
            int mid = low + ((high - low) >>> 1);
            if (child < ends.get(mid)) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        return low;
    }

    RoaringNavigableMap64 childrenOf(RoaringNavigableMap64 parents) {
        RoaringNavigableMap64 children = new RoaringNavigableMap64();
        long start = 0;
        for (int i = 0; i < rowIds.size(); i++) {
            long end = ends.get(i);
            if (parents.contains(rowIds.get(i))) {
                children.addRange(new Range(start, end - 1));
            }
            start = end;
        }
        return children;
    }

    void write(OutputStream stream, int dim) throws IOException {
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(stream));
        out.writeInt(MAGIC);
        out.writeInt(dim);
        out.writeInt(rowIds.size());
        for (int i = 0; i < rowIds.size(); i++) {
            out.writeLong(rowIds.get(i));
            out.writeLong(ends.get(i));
        }
        out.flush();
    }

    static MultiVectorIndexRows read(SeekableInputStream stream, long fileSize) throws IOException {
        DataInputStream in = new DataInputStream(new BufferedInputStream(stream));
        if (in.readInt() != MAGIC) {
            throw new IOException("Invalid multivector index header");
        }
        MultiVectorIndexRows rows = new MultiVectorIndexRows();
        rows.dimension = in.readInt();
        int count = in.readInt();
        if (rows.dimension <= 0 || count < 0 || 12L + count * 16L > fileSize) {
            throw new IOException("Invalid multivector index dimensions or mapping length");
        }
        if (count == 0 && fileSize != rows.sizeInBytes()) {
            throw new IOException("Empty multivector mapping has an unexpected index payload.");
        }
        for (int i = 0; i < count; i++) {
            long rowId = in.readLong();
            long end = in.readLong();
            if (rowId < 0 || end <= rows.childCount()) {
                throw new IOException("Invalid parent mapping in multivector index");
            }
            rows.append(rowId, end);
        }
        if (count > 0 && fileSize == rows.sizeInBytes()) {
            throw new IOException("Nonempty multivector mapping is missing its index payload.");
        }
        return rows;
    }
}
