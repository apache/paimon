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

package org.apache.paimon.utils;

import org.apache.paimon.annotation.VisibleForTesting;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;

/** A compressed bitmap for 32-bit integer. */
public class HllSketchUtil {

    public static byte[] union(byte[] sketchBytes1, byte[] sketchBytes2) {
        HllSketch heapify = HllSketch.heapify((byte[]) sketchBytes1);
        org.apache.datasketches.hll.Union union = Union.heapify((byte[]) sketchBytes2);
        union.update(heapify);
        HllSketch result = union.getResult(TgtHllType.HLL_4);
        return result.toCompactByteArray();
    }

    @VisibleForTesting
    public static byte[] sketchOf(int... values) {
        HllSketch hllSketch = new HllSketch();
        for (int value : values) {
            hllSketch.update(value);
        }
        return hllSketch.toCompactByteArray();
    }
}
