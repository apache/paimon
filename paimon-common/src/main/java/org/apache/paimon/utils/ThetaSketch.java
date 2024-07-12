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

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;

/** A compressed bitmap for 32-bit integer. */
public class ThetaSketch {

    public static byte[] union(byte[] sketchBytes1, byte[] sketchBytes2) {
        Union union = Sketches.setOperationBuilder().buildUnion();
        union.union(Memory.wrap(sketchBytes1));
        union.union(Memory.wrap(sketchBytes2));
        return union.getResult().toByteArray();
    }

    @VisibleForTesting
    public static byte[] sketchOf(int... values) {
        UpdateSketch updateSketch = UpdateSketch.builder().build();
        for (int value : values) {
            updateSketch.update(value);
        }
        return updateSketch.compact().toByteArray();
    }
}
