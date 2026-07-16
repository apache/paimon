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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A scored physical row position in a primary-key table snapshot. */
public final class PrimaryKeySearchPosition
        implements Comparable<PrimaryKeySearchPosition>, Serializable {

    private static final long serialVersionUID = 1L;

    private final BinaryRow partition;
    private final int bucket;
    private final String dataFileName;
    private final long rowPosition;
    private final float score;

    public PrimaryKeySearchPosition(
            BinaryRow partition, int bucket, String dataFileName, long rowPosition, float score) {
        checkArgument(rowPosition >= 0, "Row position must not be negative: %s.", rowPosition);
        checkArgument(
                !Float.isNaN(score) && !Float.isInfinite(score),
                "Search score must be finite: %s.",
                score);
        this.partition = Objects.requireNonNull(partition, "partition").copy();
        this.bucket = bucket;
        this.dataFileName = Objects.requireNonNull(dataFileName, "dataFileName");
        this.rowPosition = rowPosition;
        this.score = score;
    }

    public BinaryRow partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public String dataFileName() {
        return dataFileName;
    }

    public long rowPosition() {
        return rowPosition;
    }

    public float score() {
        return score;
    }

    public PrimaryKeySearchPosition withScore(float newScore) {
        return new PrimaryKeySearchPosition(partition, bucket, dataFileName, rowPosition, newScore);
    }

    @Override
    public int compareTo(PrimaryKeySearchPosition other) {
        int comparison = compareBytes(partition.toBytes(), other.partition.toBytes());
        if (comparison != 0) {
            return comparison;
        }
        comparison = Integer.compare(bucket, other.bucket);
        if (comparison != 0) {
            return comparison;
        }
        comparison = dataFileName.compareTo(other.dataFileName);
        return comparison != 0 ? comparison : Long.compare(rowPosition, other.rowPosition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PrimaryKeySearchPosition)) {
            return false;
        }
        PrimaryKeySearchPosition that = (PrimaryKeySearchPosition) o;
        return bucket == that.bucket
                && rowPosition == that.rowPosition
                && partition.equals(that.partition)
                && dataFileName.equals(that.dataFileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, bucket, dataFileName, rowPosition);
    }

    @Override
    public String toString() {
        return "PrimaryKeySearchPosition{"
                + "partition="
                + partition
                + ", bucket="
                + bucket
                + ", dataFileName='"
                + dataFileName
                + '\''
                + ", rowPosition="
                + rowPosition
                + ", score="
                + score
                + '}';
    }

    private static int compareBytes(byte[] left, byte[] right) {
        int count = Math.min(left.length, right.length);
        for (int i = 0; i < count; i++) {
            int comparison = Integer.compare(left[i] & 0xFF, right[i] & 0xFF);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }
}
