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

import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/** Row id ranges. */
public final class RowIdSequence implements Iterable<Range> {

    private final long min; // include
    private final long max; // exclude
    private final List<Range> ranges;

    /**
     * Creates a RowIdSequence with a single range from start (inclusive) to end (exclusive).
     *
     * @param start the starting row id (inclusive)
     * @param end the ending row id (exclusive)
     */
    public RowIdSequence(long start, long end) {
        Preconditions.checkArgument(end > start, "End must be greater than start.");
        this.min = start;
        this.max = end;
        ranges = Collections.singletonList(Range.of(start, end));
    }

    public RowIdSequence(List<Range> ranges) {
        Preconditions.checkArgument(
                !ranges.isEmpty(), "RowIdSequence must have at least one range.");
        this.min = ranges.get(0).getStart();
        this.max = ranges.get(ranges.size() - 1).getEnd();
        this.ranges = ranges;
    }

    public RowIdSequence(long... ranges) {
        if (ranges.length % 2 != 0) {
            throw new IllegalArgumentException("RowIdSequence must have even number of elements.");
        }
        List<Range> rangeList = new ArrayList<>();
        for (int i = 0; i < ranges.length; i += 2) {
            Preconditions.checkArgument(
                    ranges[i + 1] > ranges[i],
                    "End must be greater than start for range: ["
                            + ranges[i]
                            + ", "
                            + ranges[i + 1]
                            + "]");
            if (i != 0) {
                Preconditions.checkArgument(
                        ranges[i] > ranges[i - 1],
                        "Start of the current range must be greater than end of the previous range: "
                                + "["
                                + ranges[i - 1]
                                + ", "
                                + ranges[i]
                                + "]");
            }
            rangeList.add(Range.of(ranges[i], ranges[i + 1]));
        }
        this.min = ranges[0];
        this.max = ranges[ranges.length - 1];
        this.ranges = Collections.unmodifiableList(rangeList);
    }

    public int size() {
        return ranges.size();
    }

    public Range get(int index) {
        Preconditions.checkElementIndex(index, ranges.size(), "Index out of bounds.");
        return ranges.get(index);
    }

    @Override
    public Iterator<Range> iterator() {
        return ranges.iterator();
    }

    private boolean mayExist(long rowIndex) {
        return rowIndex >= min && rowIndex < max;
    }

    public long toRowNumber(long rowIndex) {
        if (mayExist(rowIndex)) {
            long rowNumber = 0;
            for (Range range : ranges) {
                long offset = range.offset(rowIndex);
                if (offset != -1) {
                    return rowNumber + offset;
                } else {
                    rowNumber += range.count();
                }
            }
        }
        return -1;
    }

    public List<Range> ranges() {
        return ranges;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("RowIdSequence{");
        for (Range range : this) {
            sb.append("[")
                    .append(range.getStart())
                    .append(", ")
                    .append(range.getEnd())
                    .append("],");
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(min);
        result = 31 * result + Long.hashCode(max);
        for (Range range : ranges) {
            result = 31 * result + range.hashCode();
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RowIdSequence that = (RowIdSequence) obj;
        return min == that.min && max == that.max && ranges.equals(that.ranges);
    }

    // serialize to bytes
    public byte[] serialize() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
            dataOutputStream.writeLong(ranges.size());
            for (Range range : ranges) {
                dataOutputStream.writeLong(range.getStart());
                dataOutputStream.writeLong(range.getEnd());
            }
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize RowIdSequence", e);
        }
    }

    // deserialize from bytes
    public static RowIdSequence deserialize(byte[] bytes) {
        try {
            if (bytes.length < 16) {
                throw new IllegalArgumentException("Invalid byte array length for RowIdSequence.");
            }
            DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
            long size = dataInputStream.readLong();
            List<Range> ranges = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                long start = dataInputStream.readLong();
                long end = dataInputStream.readLong();
                ranges.add(Range.of(start, end));
            }
            return new RowIdSequence(ranges);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize RowIdSequence", e);
        }
    }

    public int compareTo(@NotNull RowIdSequence o) {
        if (this.min != o.min) {
            return Long.compare(this.min, o.min);
        }
        if (this.max != o.max) {
            return Long.compare(this.max, o.max);
        }
        return 0;
    }
}
