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

package org.apache.paimon.spark.globalindex.sorted;

import org.apache.paimon.globalindex.sorted.NullBitmapBuffer;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.apache.spark.Partitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.ShuffledRDD;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.shuffle.sort.SortShuffleManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering;
import org.apache.spark.sql.execution.UnsafeRowSerializer;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.MutablePair;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import scala.Function1;
import scala.Product2;
import scala.Tuple2;
import scala.math.Ordering;
import scala.reflect.ClassTag$;

/** Reader-local scalar null aggregation; the fourth field exists only during the shuffle. */
final class NullBitmapShuffle {

    private NullBitmapShuffle() {}

    static Dataset<Row> sort(Dataset<Row> input, int partitions, boolean compress) {
        BuildPlan plan = plan(input, partitions, compress);
        return sort(input, plan.partitioner, plan.compress);
    }

    static BuildPlan plan(Dataset<Row> input, int partitions, boolean candidate) {
        CollectionAccumulator<long[]> counts =
                input.sparkSession().sparkContext().collectionAccumulator();
        Partitioner partitioner = createPartitioner(input, partitions, counts);
        return new BuildPlan(partitioner, candidate && shouldCompress(counts.value()));
    }

    static final class BuildPlan {
        final Partitioner partitioner;
        final boolean compress;

        private BuildPlan(Partitioner partitioner, boolean compress) {
            this.partitioner = partitioner;
            this.compress = compress;
        }
    }

    static boolean shouldCompress(List<long[]> partitionCounts) {
        Map<Long, long[]> distinct = new HashMap<>();
        for (long[] count : partitionCounts) {
            distinct.putIfAbsent(count[0], count);
        }
        long rows = 0;
        long nulls = 0;
        for (long[] count : distinct.values()) {
            rows += count[1];
            nulls += count[2];
        }
        return shouldCompress(rows, nulls);
    }

    static boolean shouldCompress(long rows, long nulls) {
        return rows > 0 && (double) nulls / rows >= 0.8;
    }

    static Partitioner createPartitioner(Dataset<Row> input, int partitions) {
        return createPartitioner(input, partitions, null);
    }

    private static Partitioner createPartitioner(
            Dataset<Row> input, int partitions, CollectionAccumulator<long[]> counts) {
        Ordering<InternalRow> ordering = LazilyGeneratedOrdering.forSchema(input.schema());
        JavaRDD<InternalRow> source =
                JavaRDD.fromRDD(
                        input.queryExecution().toRdd(), ClassTag$.MODULE$.apply(InternalRow.class));
        JavaRDD<Product2<InternalRow, Object>> sampled =
                source.mapPartitionsWithIndex(
                        (index, rows) ->
                                new Iterator<Product2<InternalRow, Object>>() {
                                    private final MutablePair<InternalRow, Object> pair =
                                            new MutablePair<>();
                                    private long total;
                                    private long nulls;
                                    private boolean reported;

                                    @Override
                                    public boolean hasNext() {
                                        boolean hasNext = rows.hasNext();
                                        if (!hasNext && !reported && counts != null) {
                                            // RangePartitioner may rescan oversized
                                            // partitions; count each only once.
                                            counts.add(new long[] {index, total, nulls});
                                            reported = true;
                                        }
                                        return hasNext;
                                    }

                                    @Override
                                    public Product2<InternalRow, Object> next() {
                                        InternalRow row = rows.next();
                                        total++;
                                        nulls += row.isNullAt(1) ? 1 : 0;
                                        return pair.update(row.copy(), null);
                                    }
                                },
                        false);
        // Boundaries must be sampled from source rows, not from indivisible bitmap batches.
        RangePartitioner<InternalRow, Object> ranges =
                new RangePartitioner<>(
                        partitions,
                        sampled.rdd(),
                        true,
                        Integer.parseInt(
                                input.sparkSession()
                                        .conf()
                                        .get(
                                                "spark.sql.execution.rangeExchange.sampleSizePerPartition",
                                                "100")),
                        ordering,
                        ClassTag$.MODULE$.apply(InternalRow.class));
        return new NativePartitioner(ranges, input.schema());
    }

    static Dataset<Row> sort(Dataset<Row> input, Partitioner partitioner, boolean compress) {
        if (!compress) {
            return sortUncompressed(input, partitioner);
        }
        JavaPairRDD<Row, Object> original =
                input.javaRDD().mapToPair(row -> new Tuple2<>(row, null));
        JavaPairRDD<Row, Object> transport = original;
        if (compress) {
            transport =
                    original.keys()
                            .mapPartitionsToPair(
                                    rows -> {
                                        Iterator<Row> compressed = compressRows(rows, partitioner);
                                        return new Iterator<Tuple2<Row, Object>>() {
                                            @Override
                                            public boolean hasNext() {
                                                return compressed.hasNext();
                                            }

                                            @Override
                                            public Tuple2<Row, Object> next() {
                                                return new Tuple2<>(compressed.next(), null);
                                            }
                                        };
                                    });
        }
        ShuffledRDD<Row, Object, Object> shuffled =
                new ShuffledRDD<>(
                        transport.rdd(),
                        partitioner,
                        ClassTag$.MODULE$.apply(Row.class),
                        ClassTag$.MODULE$.apply(Object.class),
                        ClassTag$.MODULE$.apply(Object.class));
        // External rows and bitmap batches must not depend on application Kryo registrations.
        shuffled.setSerializer(new JavaSerializer(input.sparkSession().sparkContext().getConf()));
        // Restore original rows before local sorting so file rotation and query layout are
        // unchanged.
        return input.sparkSession()
                .createDataFrame(
                        JavaPairRDD.fromRDD(
                                        shuffled,
                                        ClassTag$.MODULE$.apply(Row.class),
                                        ClassTag$.MODULE$.apply(Object.class))
                                .keys()
                                .mapPartitions(NullBitmapShuffle::expandRows),
                        input.schema())
                .sortWithinPartitions(
                        org.apache.spark.sql.functions.col(input.schema().fields()[0].name()),
                        org.apache.spark.sql.functions.col(input.schema().fields()[1].name()),
                        org.apache.spark.sql.functions.col(input.schema().fields()[2].name()));
    }

    /** Keep SQL's compact wire format when bitmap aggregation is not worthwhile. */
    private static Dataset<Row> sortUncompressed(Dataset<Row> input, Partitioner partitioner) {
        final StructType schema = input.schema();
        JavaRDD<InternalRow> source =
                JavaRDD.fromRDD(
                        input.queryExecution().toRdd(), ClassTag$.MODULE$.apply(InternalRow.class));
        JavaRDD<Product2<Integer, InternalRow>> assigned =
                source.mapPartitions(
                        rows -> {
                            UnsafeProjection projection = UnsafeProjection.create(schema);
                            // Like SQL's range exchange, reuse rows only when shuffle serializes
                            // them immediately rather than buffering Java objects.
                            boolean reuseRows =
                                    SparkEnv.get().shuffleManager() instanceof SortShuffleManager
                                            && partitioner.numPartitions()
                                                    <= SortShuffleManager
                                                            .MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE();
                            return new Iterator<Product2<Integer, InternalRow>>() {
                                private final MutablePair<Integer, InternalRow> pair =
                                        new MutablePair<>();

                                @Override
                                public boolean hasNext() {
                                    return rows.hasNext();
                                }

                                @Override
                                public Product2<Integer, InternalRow> next() {
                                    InternalRow row = rows.next();
                                    int target = partitioner.getPartition(row);
                                    UnsafeRow value =
                                            row instanceof UnsafeRow
                                                    ? (UnsafeRow) row
                                                    : projection.apply(row);
                                    return reuseRows
                                            ? pair.update(target, value)
                                            : new Tuple2<>(target, value.copy());
                                }
                            };
                        });
        ShuffledRDD<Integer, InternalRow, InternalRow> shuffled =
                new ShuffledRDD<>(
                        assigned.rdd(),
                        new DestinationPartitioner(partitioner.numPartitions()),
                        ClassTag$.MODULE$.apply(Integer.class),
                        ClassTag$.MODULE$.apply(InternalRow.class),
                        ClassTag$.MODULE$.apply(InternalRow.class));
        shuffled.setSerializer(new UnsafeRowSerializer(schema.size(), null));
        RDD<InternalRow> output =
                JavaPairRDD.fromRDD(
                                shuffled,
                                ClassTag$.MODULE$.apply(Integer.class),
                                ClassTag$.MODULE$.apply(InternalRow.class))
                        .values()
                        .rdd();
        try {
            // Spark 4 exposes this on the classic session rather than the abstract session API.
            Dataset<Row> frame =
                    (Dataset<Row>)
                            input.sparkSession()
                                    .getClass()
                                    .getMethod(
                                            "internalCreateDataFrame",
                                            RDD.class,
                                            StructType.class,
                                            boolean.class)
                                    .invoke(input.sparkSession(), output, schema, false);
            return frame.sortWithinPartitions(
                    org.apache.spark.sql.functions.col(schema.fields()[0].name()),
                    org.apache.spark.sql.functions.col(schema.fields()[1].name()),
                    org.apache.spark.sql.functions.col(schema.fields()[2].name()));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Cannot create a DataFrame from index shuffle rows.", e);
        }
    }

    private static final class DestinationPartitioner extends Partitioner {
        private final int partitions;

        private DestinationPartitioner(int partitions) {
            this.partitions = partitions;
        }

        @Override
        public int numPartitions() {
            return partitions;
        }

        @Override
        public int getPartition(Object key) {
            return (Integer) key;
        }
    }

    static Iterator<Row> compressRows(Iterator<Row> input) {
        return compressRows(input, null);
    }

    static Iterator<Row> compressRows(Iterator<Row> input, Partitioner partitioner) {
        return new Iterator<Row>() {
            private final NullBitmapBuffer buffer = new NullBitmapBuffer();
            private long taskId;
            private int targetPartition;
            private Row pending;
            private Row next;

            @Override
            public boolean hasNext() {
                if (next != null) {
                    return true;
                }
                try {
                    while (pending != null || input.hasNext()) {
                        if (pending == null) {
                            pending = input.next();
                        }
                        int target = partitioner == null ? 0 : partitioner.getPartition(pending);
                        if (!buffer.isEmpty()
                                && (pending.getLong(0) != taskId
                                        || (pending.isNullAt(1) && target != targetPartition))) {
                            next = drain();
                            return true;
                        }
                        Row row = pending;
                        pending = null;
                        if (!row.isNullAt(1)) {
                            // The receiving DataFrame supplies the schema; shuffle only values.
                            next = RowFactory.create(row.get(0), row.get(1), row.get(2));
                            return true;
                        }
                        taskId = row.getLong(0);
                        targetPartition = target;
                        buffer.add(row.getLong(2));
                        if (buffer.isFull()) {
                            next = drain();
                            return true;
                        }
                    }
                    if (!buffer.isEmpty()) {
                        next = drain();
                    }
                    return next != null;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            private Row drain() throws IOException {
                long first = buffer.firstRowId();
                return RowFactory.create(taskId, null, first, buffer.drain());
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Row row = next;
                next = null;
                return row;
            }
        };
    }

    /** Native range keys avoid decoding every non-null value into a Java object. */
    private static final class NativePartitioner extends Partitioner {
        private static final long serialVersionUID = 1L;
        private final RangePartitioner<InternalRow, Object> ranges;
        private final StructType schema;
        private transient Function1<Object, Object> toCatalyst;

        private NativePartitioner(RangePartitioner<InternalRow, Object> ranges, StructType schema) {
            this.ranges = ranges;
            this.schema = schema;
        }

        @Override
        public int numPartitions() {
            return ranges.numPartitions();
        }

        @Override
        public int getPartition(Object key) {
            if (key instanceof InternalRow) {
                return ranges.getPartition(key);
            }
            if (toCatalyst == null) {
                toCatalyst = CatalystTypeConverters.createToCatalystConverter(schema);
            }
            Row row = (Row) key;
            Row rangeKey =
                    row.size() == 3
                            ? row
                            : RowFactory.create(row.getLong(0), row.get(1), row.getLong(2));
            return ranges.getPartition(toCatalyst.apply(rangeKey));
        }
    }

    static Iterator<Row> expandRows(Iterator<Row> input) {
        return new Iterator<Row>() {
            private Iterator<Long> nullIds = Collections.<Long>emptyList().iterator();
            private long taskId;

            @Override
            public boolean hasNext() {
                return nullIds.hasNext() || input.hasNext();
            }

            @Override
            public Row next() {
                if (nullIds.hasNext()) {
                    return RowFactory.create(taskId, null, nullIds.next());
                }
                Row row = input.next();
                if (row.size() == 3) {
                    return row;
                }
                if (row.isNullAt(3)) {
                    return RowFactory.create(row.getLong(0), row.get(1), row.getLong(2));
                }
                RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
                try {
                    bitmap.deserialize((byte[]) row.get(3));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                taskId = row.getLong(0);
                nullIds = bitmap.iterator();
                return RowFactory.create(taskId, null, nullIds.next());
            }
        };
    }
}
