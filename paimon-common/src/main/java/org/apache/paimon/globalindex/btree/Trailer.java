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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.utils.Preconditions;

/** Trailer of a BTree index file. */
public class Trailer {
    public static final int TRAILER_LENGTH = 16;

    private final long nullBitmapOffset;
    private final int crc32c;

    public Trailer(long nullBitmapOffset, int crc32c) {
        this.nullBitmapOffset = nullBitmapOffset;
        this.crc32c = crc32c;
    }

    public int getCrc32c() {
        return crc32c;
    }

    public long getNullBitmapOffset() {
        return nullBitmapOffset;
    }

    public static Trailer readTrailer(MemorySlice trailerEncoding) {
        MemorySliceInput input = trailerEncoding.toInput();
        long nullBitmapOffset = input.readLong();
        int crc32c = input.readInt();

        int magicNumber = input.readInt();
        Preconditions.checkState(magicNumber == BTreeIndexWriter.MAGIC_NUMBER);

        return new Trailer(nullBitmapOffset, crc32c);
    }

    public static void write(Trailer trailer, MemorySliceOutput output) {
        output.writeLong(trailer.nullBitmapOffset);
        output.writeInt(trailer.crc32c);
        output.writeInt(BTreeIndexWriter.MAGIC_NUMBER);
    }
}
