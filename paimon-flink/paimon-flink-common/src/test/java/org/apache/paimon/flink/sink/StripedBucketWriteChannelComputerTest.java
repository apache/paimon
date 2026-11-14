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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.apache.paimon.flink.sink.StripedBucketWriteChannelComputer.AUTO;
import static org.apache.paimon.flink.sink.StripedBucketWriteChannelComputer.MATCH_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StripedBucketWriteChannelComputer}. */
public class StripedBucketWriteChannelComputerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testSingleWriterPerBucketEqualsRowDataChannelComputer() throws Exception {
        TableSchema schema = createNoPartitionSchema(4);
        List<InternalRow> input = randomRows(2000, 12345L);

        int numChannels = 4;
        StripedBucketWriteChannelComputer striped =
                new StripedBucketWriteChannelComputer(schema, 1);
        striped.setup(numChannels);
        RowDataChannelComputer base = new RowDataChannelComputer(schema);
        base.setup(numChannels);

        for (InternalRow row : input) {
            assertThat(striped.channel(row)).isEqualTo(base.channel(row));
        }
    }

    @Test
    public void testStripedChannelsPerBucket() throws Exception {
        int numBuckets = 4;
        int numStripes = 2;
        int numChannels = numBuckets * numStripes;
        TableSchema schema = createNoPartitionSchema(numBuckets);

        Map<Integer, Set<Integer>> channelsByBucket =
                groupByBucket(schema, randomRows(4000, 42L), numStripes, numChannels);
        assertThat(channelsByBucket.keySet()).hasSize(numBuckets);

        Set<Integer> allChannels = new HashSet<>();
        for (int bucket = 0; bucket < numBuckets; bucket++) {
            Set<Integer> channels = channelsByBucket.get(bucket);
            // each bucket is striped to exactly numStripes distinct channels
            assertThat(channels).hasSize(numStripes);
            // with numChannels = numBuckets * numStripes the per-bucket channel sets are disjoint
            // (the partition start-channel only rotates them, it does not overlap them)
            for (int channel : channels) {
                assertThat(allChannels.add(channel)).isTrue();
            }
        }
        // together they cover every channel exactly once
        assertThat(allChannels).hasSize(numChannels);
    }

    @Test
    public void testSameKeyAlwaysSameChannel() throws Exception {
        TableSchema schema = createNoPartitionSchema(4);
        StripedBucketWriteChannelComputer computer =
                new StripedBucketWriteChannelComputer(schema, 3);
        computer.setup(12);

        Random random = new Random(7L);
        for (int i = 0; i < 200; i++) {
            long key = random.nextInt(50);
            InternalRow first = GenericRow.of(key, random.nextDouble());
            int expected = computer.channel(first);
            // same key, different value -> must route to the same channel
            for (int repeat = 0; repeat < 5; repeat++) {
                InternalRow again = GenericRow.of(key, random.nextDouble());
                assertThat(computer.channel(again)).isEqualTo(expected);
            }
        }
    }

    @Test
    public void testMatchParallelismCoversAllChannels() throws Exception {
        int numBuckets = 4;
        int numChannels = 8;
        TableSchema schema = createNoPartitionSchema(numBuckets);

        // numStripes = numChannels -> every bucket is written by all channels
        Map<Integer, Set<Integer>> channelsByBucket =
                groupByBucket(schema, randomRows(4000, 99L), numChannels, numChannels);
        assertStripesPerBucket(channelsByBucket, numBuckets, numChannels);
    }

    @Test
    public void testDistributionIsBalanced() throws Exception {
        int numBuckets = 4;
        int numStripes = 2;
        int numChannels = numBuckets * numStripes;
        TableSchema schema = createNoPartitionSchema(numBuckets);
        int numRecords = 8000;
        List<InternalRow> input = randomRows(numRecords, 2026L);

        StripedBucketWriteChannelComputer computer =
                new StripedBucketWriteChannelComputer(schema, numStripes);
        computer.setup(numChannels);

        int[] count = new int[numChannels];
        for (InternalRow row : input) {
            count[computer.channel(row)]++;
        }

        int max = Arrays.stream(count).max().getAsInt();
        int min = Arrays.stream(count).min().getAsInt();
        // every channel should receive a fair share; allow 30% deviation from the mean
        assertThat(min).isGreaterThan(0);
        assertThat(max - min).isLessThanOrEqualTo((int) (numRecords / (double) numChannels * 0.3));
    }

    @Test
    public void testAutoStripesResolveFromChannelCount() throws Exception {
        int numBuckets = 4;
        int numChannels = 8; // numStripes = ceil(8/4) = 2
        TableSchema schema = createNoPartitionSchema(numBuckets);

        Map<Integer, Set<Integer>> channelsByBucket =
                groupByBucket(schema, randomRows(4000, 5L), AUTO, numChannels);
        assertStripesPerBucket(channelsByBucket, numBuckets, 2);
    }

    @Test
    public void testAutoStripesCeilWhenNotDivisible() throws Exception {
        int numBuckets = 3;
        int numChannels =
                7; // numStripes = ceil(7/3) = 3, stripes*buckets > numChannels so sets wrap
        TableSchema schema = createNoPartitionSchema(numBuckets);

        Map<Integer, Set<Integer>> channelsByBucket =
                groupByBucket(schema, randomRows(6000, 6L), AUTO, numChannels);
        // each bucket still stripes to exactly 3 distinct channels even when sets wrap
        assertStripesPerBucket(channelsByBucket, numBuckets, 3);
    }

    @Test
    public void testMatchParallelismResolvesToChannelCount() throws Exception {
        int numBuckets = 4;
        int numChannels = 8;
        TableSchema schema = createNoPartitionSchema(numBuckets);

        Map<Integer, Set<Integer>> channelsByBucket =
                groupByBucket(schema, randomRows(4000, 7L), MATCH_PARALLELISM, numChannels);
        assertStripesPerBucket(channelsByBucket, numBuckets, numChannels);
    }

    @Test
    public void testPartitionedTableStripes() throws Exception {
        int numBuckets = 2;
        int numStripes = 2;
        int numChannels = numBuckets * numStripes;
        TableSchema schema = createPartitionedSchema(numBuckets);
        List<InternalRow> input = new ArrayList<>();
        Random random = new Random(11L);
        for (int i = 0; i < 4000; i++) {
            input.add(GenericRow.of(random.nextInt(3) + 1, random.nextLong(), random.nextDouble()));
        }

        StripedBucketWriteChannelComputer computer =
                new StripedBucketWriteChannelComputer(schema, numStripes);
        computer.setup(numChannels);
        FixedBucketRowKeyExtractor extractor = new FixedBucketRowKeyExtractor(schema);

        // each (partition, bucket) pair must still stripe to exactly numStripes distinct channels
        Map<String, Set<Integer>> channelsByPartitionBucket = new HashMap<>();
        for (InternalRow row : input) {
            extractor.setRecord(row);
            BinaryRow partition = extractor.partition();
            int bucket = extractor.bucket();
            int channel = computer.channel(row);
            assertThat(channel).isBetween(0, numChannels - 1);
            channelsByPartitionBucket
                    .computeIfAbsent(partition.hashCode() + ":" + bucket, k -> new HashSet<>())
                    .add(channel);
        }
        for (Set<Integer> channels : channelsByPartitionBucket.values()) {
            assertThat(channels).hasSize(numStripes);
        }
    }

    @Test
    public void testAppendAlternatingBucketsFanOut() throws Exception {
        int numBuckets = 2;
        int numStripes = 2;
        int numChannels = numBuckets * numStripes;
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        TableSchema schema =
                createSchema(
                        rowType,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        numBuckets,
                        Collections.singletonMap("bucket-key", "k"));

        // one fixed bucket-key value per bucket: a low-cardinality key must still fan out
        FixedBucketRowKeyExtractor extractor = new FixedBucketRowKeyExtractor(schema);
        int key0 = -1;
        int key1 = -1;
        for (int x = 0; x < 1000 && (key0 < 0 || key1 < 0); x++) {
            extractor.setRecord(GenericRow.of(x, 0));
            int bucket = extractor.bucket();
            if (bucket == 0 && key0 < 0) {
                key0 = x;
            } else if (bucket == 1 && key1 < 0) {
                key1 = x;
            }
        }
        assertThat(key0).isGreaterThanOrEqualTo(0);
        assertThat(key1).isGreaterThanOrEqualTo(0);

        StripedBucketWriteChannelComputer computer =
                new StripedBucketWriteChannelComputer(schema, numStripes);
        computer.setup(numChannels);

        // alternate the two buckets, each with a single fixed key; round-robin per bucket must fan
        // each bucket out to every stripe (a bucket-key hash would pin each bucket to one writer)
        Map<Integer, Set<Integer>> channelsByBucket = new HashMap<>();
        for (int i = 0; i < 2000; i++) {
            InternalRow row = GenericRow.of(i % 2 == 0 ? key0 : key1, i);
            extractor.setRecord(row);
            int bucket = extractor.bucket();
            int channel = computer.channel(row);
            assertThat(channel).isBetween(0, numChannels - 1);
            channelsByBucket.computeIfAbsent(bucket, k -> new HashSet<>()).add(channel);
        }
        assertStripesPerBucket(channelsByBucket, numBuckets, numStripes);
    }

    @Test
    public void testEqualNaNKeysRouteToSameChannel() throws Exception {
        int numBuckets = 2;
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.DOUBLE()},
                        new String[] {"tenant", "k"});
        TableSchema schema =
                createSchema(
                        rowType,
                        Collections.emptyList(),
                        Arrays.asList("tenant", "k"),
                        numBuckets,
                        Collections.singletonMap("bucket-key", "tenant"));

        // two quiet-NaN payloads that compare equal as primary keys but differ in raw bits
        double nanA = Double.longBitsToDouble(0x7ff8000000000001L);
        double nanB = Double.longBitsToDouble(0x7ff8000000000003L);
        assertThat(Double.compare(nanA, nanB)).isEqualTo(0);
        InternalRow a = GenericRow.of(1, nanA);
        InternalRow b = GenericRow.of(1, nanB);

        for (int numStripes = 2; numStripes <= 5; numStripes++) {
            StripedBucketWriteChannelComputer computer =
                    new StripedBucketWriteChannelComputer(schema, numStripes);
            computer.setup(numBuckets * numStripes);
            // equal primary keys must never be split across writers, or they acquire independent
            // sequence streams and a stale upsert can win
            assertThat(computer.channel(a)).isEqualTo(computer.channel(b));
        }
    }

    @Test
    public void testAppendSingleStripeEqualsRowDataChannelComputer() throws Exception {
        int numBuckets = 4;
        int numChannels = 4;
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        TableSchema schema =
                createSchema(
                        rowType,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        numBuckets,
                        Collections.singletonMap("bucket-key", "k"));

        // a single writer per bucket needs no round-robin state and degenerates to bucket routing
        StripedBucketWriteChannelComputer striped =
                new StripedBucketWriteChannelComputer(schema, 1);
        striped.setup(numChannels);
        RowDataChannelComputer base = new RowDataChannelComputer(schema);
        base.setup(numChannels);

        Random random = new Random(31L);
        for (int i = 0; i < 2000; i++) {
            InternalRow row = GenericRow.of(random.nextInt(1000), i);
            assertThat(striped.channel(row)).isEqualTo(base.channel(row));
        }
    }

    @Test
    public void testPartitionedAppendFanOut() throws Exception {
        int numBuckets = 2;
        int numStripes = 2;
        int numChannels = numBuckets * numStripes;
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT()},
                        new String[] {"pt", "k", "v"});
        TableSchema schema =
                createSchema(
                        rowType,
                        Collections.singletonList("pt"),
                        Collections.emptyList(),
                        numBuckets,
                        Collections.singletonMap("bucket-key", "k"));

        StripedBucketWriteChannelComputer computer =
                new StripedBucketWriteChannelComputer(schema, numStripes);
        computer.setup(numChannels);
        FixedBucketRowKeyExtractor extractor = new FixedBucketRowKeyExtractor(schema);

        // round-robin per bucket must fan each bucket out across writers even with partitions
        Random random = new Random(77L);
        Map<Integer, Set<Integer>> channelsByBucket = new HashMap<>();
        for (int i = 0; i < 6000; i++) {
            InternalRow row = GenericRow.of(random.nextInt(5), random.nextInt(1000), i);
            extractor.setRecord(row);
            int bucket = extractor.bucket();
            int channel = computer.channel(row);
            assertThat(channel).isBetween(0, numChannels - 1);
            channelsByBucket.computeIfAbsent(bucket, k -> new HashSet<>()).add(channel);
        }
        for (Set<Integer> channels : channelsByBucket.values()) {
            assertThat(channels.size()).isGreaterThanOrEqualTo(numStripes);
        }
    }

    private Map<Integer, Set<Integer>> groupByBucket(
            TableSchema schema, List<InternalRow> input, int writesPerBucket, int numChannels) {
        StripedBucketWriteChannelComputer computer =
                new StripedBucketWriteChannelComputer(schema, writesPerBucket);
        computer.setup(numChannels);
        FixedBucketRowKeyExtractor extractor = new FixedBucketRowKeyExtractor(schema);

        Map<Integer, Set<Integer>> channelsByBucket = new HashMap<>();
        for (InternalRow row : input) {
            extractor.setRecord(row);
            int channel = computer.channel(row);
            assertThat(channel).isBetween(0, numChannels - 1);
            channelsByBucket.computeIfAbsent(extractor.bucket(), k -> new HashSet<>()).add(channel);
        }
        return channelsByBucket;
    }

    private void assertStripesPerBucket(
            Map<Integer, Set<Integer>> channelsByBucket, int numBuckets, int expectedChannels) {
        for (int bucket = 0; bucket < numBuckets; bucket++) {
            assertThat(channelsByBucket.get(bucket)).hasSize(expectedChannels);
        }
    }

    private TableSchema createNoPartitionSchema(int bucket) throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.DOUBLE()},
                        new String[] {"k", "v"});
        return createSchema(
                rowType, Collections.emptyList(), Collections.singletonList("k"), bucket);
    }

    private TableSchema createPartitionedSchema(int bucket) throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT(), DataTypes.DOUBLE()},
                        new String[] {"pt", "k", "v"});
        return createSchema(
                rowType, Collections.singletonList("pt"), Arrays.asList("pt", "k"), bucket);
    }

    private TableSchema createSchema(
            RowType rowType, List<String> partitionKeys, List<String> primaryKeys, int bucket)
            throws Exception {
        return createSchema(rowType, partitionKeys, primaryKeys, bucket, Collections.emptyMap());
    }

    private TableSchema createSchema(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            int bucket,
            Map<String, String> extraOptions)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", String.valueOf(bucket));
        options.putAll(extraOptions);
        SchemaManager schemaManager =
                new FileSystemSchemaManager(LocalFileIO.create(), new Path(tempDir.toString()));
        return schemaManager.createTable(
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, options, ""));
    }

    private List<InternalRow> randomRows(int count, long seed) {
        Random random = new Random(seed);
        List<InternalRow> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            rows.add(GenericRow.of(random.nextLong(), random.nextDouble()));
        }
        return rows;
    }
}
