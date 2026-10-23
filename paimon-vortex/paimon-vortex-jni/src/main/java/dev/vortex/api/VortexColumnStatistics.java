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

package dev.vortex.api;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * Statistics of one top-level column of a written Vortex file.
 *
 * <p>The native writer calls the constructor through JNI, so its signature must not change.
 */
public final class VortexColumnStatistics {

    private final int columnIndex;
    private final long compressedSize;
    private final long valueCount;
    private final long nullValueCount;
    private final long nanValueCount;
    private final Object lowerBound;
    private final Object upperBound;

    public VortexColumnStatistics(
            int columnIndex,
            long compressedSize,
            long valueCount,
            long nullValueCount,
            long nanValueCount,
            Object lowerBound,
            Object upperBound) {
        this.columnIndex = columnIndex;
        this.compressedSize = compressedSize;
        this.valueCount = valueCount;
        this.nullValueCount = nullValueCount;
        this.nanValueCount = nanValueCount;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    /** Zero-based position of this column in the writer's Arrow schema. */
    public int columnIndex() {
        return columnIndex;
    }

    /** Compressed bytes of this column's physical layout. */
    public long compressedSize() {
        return compressedSize;
    }

    public long valueCount() {
        return valueCount;
    }

    /** Exact null count, or empty when Vortex did not compute one. */
    public OptionalLong nullValueCount() {
        return nullValueCount >= 0 ? OptionalLong.of(nullValueCount) : OptionalLong.empty();
    }

    /** Exact NaN count, or empty for columns that are not floating point. */
    public OptionalLong nanValueCount() {
        return nanValueCount >= 0 ? OptionalLong.of(nanValueCount) : OptionalLong.empty();
    }

    /**
     * Lower bound in the Java type of the Arrow storage scalar: {@link Boolean}, {@link Integer}
     * (also for 8 and 16 bit integers), {@link Long}, {@link java.math.BigInteger}, {@link Float},
     * {@link Double}, {@link java.math.BigDecimal}, {@link String} or {@code byte[]}. Bounds of
     * strings and binaries can be truncated, but they are still bounds.
     */
    public Optional<Object> lowerBound() {
        return Optional.ofNullable(lowerBound);
    }

    /** Upper bound. See {@link #lowerBound()}. */
    public Optional<Object> upperBound() {
        return Optional.ofNullable(upperBound);
    }
}
