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

package org.apache.paimon.format;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * A simple column statistics, supports the following stats.
 *
 * <ul>
 *   <li>min: the minimum value of the column
 *   <li>max: the maximum value of the column
 *   <li>nullCount: the number of nulls
 * </ul>
 */
public class SimpleColStats {

    public static final SimpleColStats NONE = new SimpleColStats(null, null, null);

    @Nullable private final Object min;
    @Nullable private final Object max;
    private final Long nullCount;

    public SimpleColStats(@Nullable Object min, @Nullable Object max, @Nullable Long nullCount) {
        this.min = min;
        this.max = max;
        this.nullCount = nullCount;
    }

    @Nullable
    public Object min() {
        return min;
    }

    @Nullable
    public Object max() {
        return max;
    }

    @Nullable
    public Long nullCount() {
        return nullCount;
    }

    public boolean isNone() {
        return min == null && max == null && nullCount == null;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SimpleColStats)) {
            return false;
        }
        SimpleColStats that = (SimpleColStats) o;
        return Objects.equals(min, that.min)
                && Objects.equals(max, that.max)
                && Objects.equals(nullCount, that.nullCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max, nullCount);
    }

    @Override
    public String toString() {
        return String.format("{%s, %s, %d}", min, max, nullCount);
    }
}
