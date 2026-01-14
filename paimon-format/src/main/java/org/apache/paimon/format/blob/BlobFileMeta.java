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

package org.apache.paimon.format.blob;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.memory.BytesUtils;
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;

/** Metadata of blob file. */
public class BlobFileMeta {

    private final long[] blobLengths;
    private final long[] blobOffsets;
    private final @Nullable int[] returnedPositions;

    public BlobFileMeta(SeekableInputStream in, long fileSize, @Nullable RoaringBitmap32 selection)
            throws IOException {
        in.seek(fileSize - 5);
        byte[] header = new byte[5];
        IOUtils.readFully(in, header);
        byte version = header[4];
        if (version != 1) {
            throw new IOException("Unsupported version: " + version);
        }
        int indexLength = BytesUtils.getInt(header, 0);

        in.seek(fileSize - 5 - indexLength);
        byte[] indexBytes = new byte[indexLength];
        IOUtils.readFully(in, indexBytes);

        long[] blobLengths = DeltaVarintCompressor.decompress(indexBytes);
        long[] blobOffsets = new long[blobLengths.length];
        long offset = 0;
        for (int i = 0; i < blobLengths.length; i++) {
            blobOffsets[i] = offset;
            offset += blobLengths[i];
        }

        int[] returnedPositions = null;
        if (selection != null) {
            int cardinality = (int) selection.getCardinality();
            returnedPositions = new int[cardinality];
            long[] newLengths = new long[cardinality];
            long[] newOffsets = new long[cardinality];
            Iterator<Integer> iterator = selection.iterator();
            for (int i = 0; i < cardinality; i++) {
                Integer next = iterator.next();
                newLengths[i] = blobLengths[next];
                newOffsets[i] = blobOffsets[next];
                returnedPositions[i] = next;
            }
            blobLengths = newLengths;
            blobOffsets = newOffsets;
        }

        this.returnedPositions = returnedPositions;
        this.blobLengths = blobLengths;
        this.blobOffsets = blobOffsets;
    }

    public long blobLength(int i) {
        return blobLengths[i];
    }

    public long blobOffset(int i) {
        return blobOffsets[i];
    }

    public int returnedPosition(int i) {
        return returnedPositions == null ? i : returnedPositions[i - 1];
    }

    public int recordNumber() {
        return blobLengths.length;
    }
}
