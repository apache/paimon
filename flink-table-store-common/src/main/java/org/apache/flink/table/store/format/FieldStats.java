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

package org.apache.flink.table.store.format;

import javax.annotation.Nullable;

import java.util.Objects;

/** Statistics for each field. */
public class FieldStats {

    @Nullable private final Object minValue;
    @Nullable private final Object maxValue;
    private final long nullCount;

    public FieldStats(@Nullable Object minValue, @Nullable Object maxValue, long nullCount) {
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.nullCount = nullCount;
    }

    public Object minValue() {
        return minValue;
    }

    public Object maxValue() {
        return maxValue;
    }

    public long nullCount() {
        return nullCount;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FieldStats)) {
            return false;
        }
        FieldStats that = (FieldStats) o;
        return Objects.equals(minValue, that.minValue)
                && Objects.equals(maxValue, that.maxValue)
                && nullCount == that.nullCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minValue, maxValue, nullCount);
    }

    @Override
    public String toString() {
        return String.format("{%s, %s, %d}", minValue, maxValue, nullCount);
    }
}
