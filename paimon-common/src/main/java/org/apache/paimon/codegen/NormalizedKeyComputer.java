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

package org.apache.paimon.codegen;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.memory.MemorySegment;

/**
 * Normalized key computer for {@code SortBuffer}. For performance, subclasses are usually
 * implemented through CodeGenerator.
 */
public interface NormalizedKeyComputer {

    /** Writes a normalized key for the given record into the target {@link MemorySegment}. */
    void putKey(InternalRow record, MemorySegment target, int offset);

    /** Compares two normalized keys in respective {@link MemorySegment}. */
    int compareKey(MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ);

    /** Swaps two normalized keys in respective {@link MemorySegment}. */
    void swapKey(MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ);

    /** Get normalized keys bytes length. */
    int getNumKeyBytes();

    /** whether the normalized key can fully determines the comparison. */
    boolean isKeyFullyDetermines();

    /** Flag whether normalized key comparisons should be inverted key. */
    boolean invertKey();
}
