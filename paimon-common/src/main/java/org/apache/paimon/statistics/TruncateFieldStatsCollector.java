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

package org.apache.paimon.statistics;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.utils.Preconditions;

import java.util.regex.Pattern;

/**
 * The truncate stats collector which will report null count, truncated min/max value. Currently,
 * truncation only performs on the {@link BinaryString} value.
 */
public class TruncateFieldStatsCollector extends AbstractFieldStatsCollector {

    public static final Pattern TRUNCATE_PATTERN = Pattern.compile("TRUNCATE\\((\\d+)\\)");

    private final int length;

    private boolean failed = false;

    public TruncateFieldStatsCollector(int length) {
        Preconditions.checkArgument(length > 0, "Truncate length should larger than zero.");
        this.length = length;
    }

    public int getLength() {
        return length;
    }

    @Override
    public void collect(Object field, Serializer<Object> fieldSerializer) {
        if (field == null) {
            nullCount++;
            return;
        }

        // TODO use comparator for not comparable types and extract this logic to a util class
        if (!(field instanceof Comparable)) {
            return;
        }

        Comparable<Object> c = (Comparable<Object>) field;
        if (minValue == null || c.compareTo(minValue) < 0) {
            minValue = fieldSerializer.copy(truncateMin(field));
        }
        if (maxValue == null || c.compareTo(maxValue) > 0) {
            Object max = truncateMax(field);
            // may fail
            if (max != null) {
                maxValue = fieldSerializer.copy(truncateMax(field));
            }
        }
    }

    @Override
    public FieldStats convert(FieldStats source) {
        Object min = truncateMin(source.minValue());
        Object max = truncateMax(source.maxValue());
        if (max == null) {
            return new FieldStats(null, null, source.nullCount());
        }
        return new FieldStats(min, max, source.nullCount());
    }

    @Override
    public FieldStats result() {
        if (failed) {
            return new FieldStats(null, null, nullCount);
        }
        return new FieldStats(minValue, maxValue, nullCount);
    }

    /** @return a truncated value less or equal than the old value. */
    private Object truncateMin(Object field) {
        if (field == null) {
            return null;
        }
        if (field instanceof BinaryString) {
            return ((BinaryString) field).substring(0, length);
        } else {
            return field;
        }
    }

    /** @return a value greater or equal than the old value. */
    private Object truncateMax(Object field) {
        if (field == null) {
            return null;
        }
        if (field instanceof BinaryString) {
            BinaryString original = ((BinaryString) field);
            BinaryString truncated = original.substring(0, length);

            // No need to increment if the input length is under the truncate length
            if (original.getSizeInBytes() == truncated.getSizeInBytes()) {
                return field;
            }

            StringBuilder truncatedStringBuilder = new StringBuilder(truncated.toString());

            // Try incrementing the code points from the end
            for (int i = length - 1; i >= 0; i--) {
                // Get the offset in the truncated string buffer where the number of unicode
                // characters = i
                int offsetByCodePoint = truncatedStringBuilder.offsetByCodePoints(0, i);
                int nextCodePoint = truncatedStringBuilder.codePointAt(offsetByCodePoint) + 1;
                // No overflow
                if (nextCodePoint != 0 && Character.isValidCodePoint(nextCodePoint)) {
                    truncatedStringBuilder.setLength(offsetByCodePoint);
                    // Append next code point to the truncated substring
                    truncatedStringBuilder.appendCodePoint(nextCodePoint);
                    return BinaryString.fromString(truncatedStringBuilder.toString());
                }
            }
            failed = true;
            return null; // Cannot find a valid upper bound
        } else {
            return field;
        }
    }
}
