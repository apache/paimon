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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.utils.MurmurHashUtils;

/**
 * A {@link RowDataChannelComputer} that stripes each bucket's writes across multiple writer
 * channels, so that write parallelism can exceed the bucket count without rescaling the table.
 *
 * <p>Each bucket is routed to several channels. For a primary-key table the record's primary key
 * picks the channel, so records sharing a key still land on the same channel; for an append-only
 * table records are distributed round-robin. A single writer per bucket is equivalent to {@link
 * RowDataChannelComputer}.
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

    private transient int numStripes;
    private transient int nextStripe;

    public StripedBucketWriteChannelComputer(TableSchema schema, int writesPerBucket) {
        super(schema);
        this.writesPerBucket = writesPerBucket;
        this.numBuckets = new CoreOptions(schema.options()).bucket();
        this.hasPrimaryKey = !schema.primaryKeys().isEmpty();
    }

    @Override
    public void setup(int numChannels) {
        super.setup(numChannels);
        this.numStripes = resolveNumStripes();
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
        int stripe =
                hasPrimaryKey
                        ? keyStripe(extractor.trimmedPrimaryKey())
                        : Math.floorMod(nextStripe++, numStripes);
        // Widen to long so that bucket * numStripes cannot overflow for large bucket counts
        long bucketSlot = (long) bucket * numStripes + stripe;
        int startChannel = ChannelComputer.startChannel(partition, numChannels);
        return (int) ((startChannel + bucketSlot) % numChannels);
    }

    private int keyStripe(BinaryRow key) {
        // Avalanche the key hash so the stripe is independent of the bucket modulo, which also
        // derives from the key hash; otherwise shared factors between numStripes and numBuckets
        // skew it.
        return Math.floorMod(MurmurHashUtils.fmix(key.hashCode()), numStripes);
    }

    @Override
    public String toString() {
        return "striped bucket shuffle";
    }
}
