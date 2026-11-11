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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MurmurHashUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link RowDataChannelComputer} that stripes each bucket's writes across multiple writer
 * channels, so that write parallelism can exceed the bucket count without rescaling the table.
 *
 * <p>Each bucket is routed to several channels. For a primary-key table the record's primary key
 * picks the channel, so records sharing a key still land on the same channel; for an append-only
 * table the bucket key is hashed to pick a channel, so records sharing a bucket key stay together.
 * A single writer per bucket is equivalent to {@link RowDataChannelComputer}.
 */
public class StripedBucketWriteChannelComputer extends RowDataChannelComputer {

    private static final long serialVersionUID = 1L;

    /** Derive the number of stripes from the channel count and the bucket count. */
    public static final int AUTO = 0;

    /** Route every bucket to all channels, so every writer writes every bucket. */
    public static final int MATCH_PARALLELISM = -1;

    private final int writesPerBucket;
    private final int numBuckets;
    private final boolean hasPrimaryKey;
    private final RowType rowType;
    private final int[] bucketKeyMapping;

    // Positions of FLOAT/DOUBLE fields in the trimmed primary key, whose NaN payloads must be
    // canonicalized before hashing so that keys equal under the key comparator hash alike.
    private final int[] floatingKeyPositions;
    private final boolean[] floatingKeyIsDouble;

    private transient int numStripes;
    // Append-only routing hashes the bucket key, the same key the bucket assignment hashes, so it
    // adds no skew beyond what bucketing already has. The projection reuses one output row.
    private transient Projection bucketKeyProjection;

    public StripedBucketWriteChannelComputer(TableSchema schema, int writesPerBucket) {
        super(schema);
        this.writesPerBucket = writesPerBucket;
        this.numBuckets = new CoreOptions(schema.options()).bucket();
        this.hasPrimaryKey = !schema.primaryKeys().isEmpty();
        this.rowType = schema.logicalRowType();
        this.bucketKeyMapping = schema.projection(schema.bucketKeys());

        List<Integer> positions = new ArrayList<>();
        List<Boolean> isDouble = new ArrayList<>();
        if (hasPrimaryKey) {
            RowType keyType = schema.logicalTrimmedPrimaryKeysType();
            for (int i = 0; i < keyType.getFieldCount(); i++) {
                DataTypeRoot root = keyType.getTypeAt(i).getTypeRoot();
                if (root == DataTypeRoot.DOUBLE) {
                    positions.add(i);
                    isDouble.add(true);
                } else if (root == DataTypeRoot.FLOAT) {
                    positions.add(i);
                    isDouble.add(false);
                }
            }
        }
        this.floatingKeyPositions = new int[positions.size()];
        this.floatingKeyIsDouble = new boolean[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            this.floatingKeyPositions[i] = positions.get(i);
            this.floatingKeyIsDouble[i] = isDouble.get(i);
        }
    }

    @Override
    public void setup(int numChannels) {
        super.setup(numChannels);
        this.numStripes = resolveNumStripes();
        if (!hasPrimaryKey && numStripes > 1) {
            this.bucketKeyProjection = CodeGenUtils.newProjection(rowType, bucketKeyMapping);
        }
    }

    private int resolveNumStripes() {
        if (writesPerBucket > 0) {
            return writesPerBucket;
        }
        if (writesPerBucket == MATCH_PARALLELISM) {
            return numChannels;
        }
        // AUTO: ceil(numChannels / numBuckets), but never less than one writer per bucket
        return Math.max(1, (numChannels + numBuckets - 1) / numBuckets);
    }

    @Override
    public int channel(InternalRow record) {
        extractor.setRecord(record);
        BinaryRow partition = extractor.partition();
        int bucket = extractor.bucket();
        int stripe;
        if (numStripes == 1) {
            stripe = 0;
        } else if (hasPrimaryKey) {
            stripe = keyStripe(extractor.trimmedPrimaryKey());
        } else {
            // Hash the bucket key, the same key bucketing hashes, so records of one bucket key stay
            // together. Avalanche before the modulo so the stripe is independent of the bucket
            // (otherwise a stripe count dividing the bucket count would fix one stripe per bucket).
            stripe =
                    Math.floorMod(
                            MurmurHashUtils.fmix(bucketKeyProjection.apply(record).hashCode()),
                            numStripes);
        }
        // Widen to long so that bucket * numStripes cannot overflow for large bucket counts
        long bucketSlot = (long) bucket * numStripes + stripe;
        int startChannel = ChannelComputer.startChannel(partition, numChannels);
        return (int) ((startChannel + bucketSlot) % numChannels);
    }

    private int keyStripe(BinaryRow key) {
        // Avalanche the key hash so the stripe is independent of the bucket modulo, which also
        // derives from the key hash; otherwise shared factors between numStripes and numBuckets
        // skew it.
        return Math.floorMod(MurmurHashUtils.fmix(canonicalKeyHash(key)), numStripes);
    }

    private int canonicalKeyHash(BinaryRow key) {
        if (floatingKeyPositions.length == 0) {
            return key.hashCode();
        }
        // The key comparator treats all NaN payloads as equal, but BinaryRow.hashCode hashes the
        // raw bits. Canonicalize NaNs on a copy so logically equal keys route to the same writer
        // and cannot acquire independent sequence streams; the written record is left unchanged.
        BinaryRow normalized = key.copy();
        for (int i = 0; i < floatingKeyPositions.length; i++) {
            int pos = floatingKeyPositions[i];
            if (normalized.isNullAt(pos)) {
                continue;
            }
            if (floatingKeyIsDouble[i]) {
                if (Double.isNaN(normalized.getDouble(pos))) {
                    normalized.setDouble(pos, Double.NaN);
                }
            } else {
                if (Float.isNaN(normalized.getFloat(pos))) {
                    normalized.setFloat(pos, Float.NaN);
                }
            }
        }
        return normalized.hashCode();
    }

    @Override
    public String toString() {
        return "striped bucket shuffle";
    }
}
