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

import org.apache.flink.table.data.binary.BinaryStringData;

import java.util.Arrays;

import static org.apache.flink.table.data.binary.BinaryStringData.fromBytes;

/** Utils for {@link BinaryStringData}. */
public class StringUtils {

    /**
     * Concatenates input strings together into a single string. Returns NULL if any argument is
     * NULL.
     */
    public static BinaryStringData concat(BinaryStringData... inputs) {
        return concat(Arrays.asList(inputs));
    }

    public static BinaryStringData concat(Iterable<BinaryStringData> inputs) {
        // Compute the total length of the result.
        int totalLength = 0;
        for (BinaryStringData input : inputs) {
            if (input == null) {
                return null;
            }

            input.ensureMaterialized();
            totalLength += input.getSizeInBytes();
        }

        // Allocate a new byte array, and copy the inputs one by one into it.
        final byte[] result = new byte[totalLength];
        int offset = 0;
        for (BinaryStringData input : inputs) {
            if (input != null) {
                int len = input.getSizeInBytes();
                SegmentsUtil.copyToBytes(
                        input.getSegments(), input.getOffset(), result, offset, len);
                offset += len;
            }
        }
        return fromBytes(result);
    }
}
