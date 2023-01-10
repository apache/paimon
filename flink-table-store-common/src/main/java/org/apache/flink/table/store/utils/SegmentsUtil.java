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

package org.apache.flink.table.store.utils;

import org.apache.flink.core.memory.MemorySegment;

/** Util for data format segments calc. */
public class SegmentsUtil {

    /** Is it just in first MemorySegment, we use quick way to do something. */
    private static boolean inFirstSegment(MemorySegment[] segments, int offset, int numBytes) {
        return numBytes + offset <= segments[0].size();
    }

    /**
     * Copy segments to a new byte[].
     *
     * @param segments Source segments.
     * @param offset Source segments offset.
     * @param numBytes the number bytes to copy.
     */
    public static byte[] copyToBytes(MemorySegment[] segments, int offset, int numBytes) {
        return copyToBytes(segments, offset, new byte[numBytes], 0, numBytes);
    }

    /**
     * Copy segments to target byte[].
     *
     * @param segments Source segments.
     * @param offset Source segments offset.
     * @param bytes target byte[].
     * @param bytesOffset target byte[] offset.
     * @param numBytes the number bytes to copy.
     */
    public static byte[] copyToBytes(
            MemorySegment[] segments, int offset, byte[] bytes, int bytesOffset, int numBytes) {
        if (inFirstSegment(segments, offset, numBytes)) {
            segments[0].get(offset, bytes, bytesOffset, numBytes);
        } else {
            copyMultiSegmentsToBytes(segments, offset, bytes, bytesOffset, numBytes);
        }
        return bytes;
    }

    public static void copyMultiSegmentsToBytes(
            MemorySegment[] segments, int offset, byte[] bytes, int bytesOffset, int numBytes) {
        int remainSize = numBytes;
        for (MemorySegment segment : segments) {
            int remain = segment.size() - offset;
            if (remain > 0) {
                int nCopy = Math.min(remain, remainSize);
                segment.get(offset, bytes, numBytes - remainSize + bytesOffset, nCopy);
                remainSize -= nCopy;
                // next new segment.
                offset = 0;
                if (remainSize == 0) {
                    return;
                }
            } else {
                // remain is negative, let's advance to next segment
                // now the offset = offset - segmentSize (-remain)
                offset = -remain;
            }
        }
    }
}
