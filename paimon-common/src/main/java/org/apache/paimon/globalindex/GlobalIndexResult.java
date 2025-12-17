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

package org.apache.paimon.globalindex;

import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.util.List;
import java.util.function.Supplier;

/** Global index result represents row ids as a compressed bitmap. */
public interface GlobalIndexResult {

    /** Returns the bitmap representing row ids. */
    RoaringNavigableMap64 results();

    default GlobalIndexResult offset(long startOffset) {
        if (startOffset == 0) {
            return this;
        }
        RoaringNavigableMap64 roaringNavigableMap64 = results();
        final RoaringNavigableMap64 roaringNavigableMap64Offset = new RoaringNavigableMap64();

        for (long rowId : roaringNavigableMap64) {
            roaringNavigableMap64Offset.add(rowId + startOffset);
        }
        return create(() -> roaringNavigableMap64Offset);
    }

    /**
     * Returns the intersection of this result and the other result.
     *
     * <p>Uses native bitmap AND operation for optimal performance.
     */
    default GlobalIndexResult and(GlobalIndexResult other) {
        return create(() -> RoaringNavigableMap64.and(this.results(), other.results()));
    }

    /**
     * Returns the union of this result and the other result.
     *
     * <p>Uses native bitmap OR operation for optimal performance.
     */
    default GlobalIndexResult or(GlobalIndexResult other) {
        return create(() -> RoaringNavigableMap64.or(this.results(), other.results()));
    }

    /** Returns an empty {@link GlobalIndexResult}. */
    static GlobalIndexResult createEmpty() {
        return create(RoaringNavigableMap64::new);
    }

    /** Returns a new {@link GlobalIndexResult} from supplier. */
    static GlobalIndexResult create(Supplier<RoaringNavigableMap64> supplier) {
        LazyField<RoaringNavigableMap64> lazyField = new LazyField<>(supplier);
        return lazyField::get;
    }

    /** Returns a new {@link GlobalIndexResult} from {@link Range}. */
    static GlobalIndexResult fromRange(Range range) {
        return create(
                () -> {
                    RoaringNavigableMap64 result64 = new RoaringNavigableMap64();
                    result64.addRange(range);
                    return result64;
                });
    }

    static GlobalIndexResult fromRanges(List<Range> ranges) {
        return create(
                () -> {
                    RoaringNavigableMap64 result64 = new RoaringNavigableMap64();
                    for (Range range : ranges) {
                        result64.addRange(range);
                    }
                    return result64;
                });
    }
}
