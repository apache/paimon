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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * An interface for building the {@link TableScan} and {@link TableRead}.
 *
 * <p>Example of distributed reading:
 *
 * <pre>{@code
 * // 1. Create a ReadBuilder (Serializable)
 * Table table = catalog.getTable(...);
 * ReadBuilder builder = table.newReadBuilder()
 *     .withFilter(...)
 *     .withProjection(...);
 *
 * // 2. Plan splits in 'Coordinator' (or named 'Driver'):
 * List<Split> splits = builder.newScan().plan().splits();
 *
 * // 3. Distribute these splits to different tasks
 *
 * // 4. Read a split in task
 * TableRead read = builder.newRead();
 * RecordReader<InternalRow> reader = read.createReader(split);
 * reader.forEachRemaining(...);
 *
 * }</pre>
 *
 * <p>{@link #newStreamScan()} will create a stream scan, which can perform continuously planning:
 *
 * <pre>{@code
 * TableScan scan = builder.newStreamScan();
 * while (true) {
 *     List<Split> splits = scan.plan().splits();
 *     ...
 * }
 * }</pre>
 *
 * <p>NOTE: {@link InternalRow} cannot be saved in memory. It may be reused internally, so you need
 * to convert it into your own data structure or copy it.
 *
 * @since 0.4.0
 */
@Public
public interface ReadBuilder extends Serializable {

    /** A name to identify the table. */
    String tableName();

    /** Returns read row type, projected by {@link #withProjection}. */
    RowType readType();

    /**
     * Apply filters to the readers to decrease the number of produced records.
     *
     * <p>This interface filters records as much as possible, however some produced records may not
     * satisfy all predicates. Users need to recheck all records.
     */
    default ReadBuilder withFilter(List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return this;
        }
        return withFilter(PredicateBuilder.and(predicates));
    }

    /**
     * Push filters, will filter the data as much as possible, but it is not guaranteed that it is a
     * complete filter.
     */
    ReadBuilder withFilter(Predicate predicate);

    /** Push partition filter. */
    ReadBuilder withPartitionFilter(Map<String, String> partitionSpec);

    /**
     * Push bucket filter. Note that this method cannot be used simultaneously with {@link
     * #withShard(int, int)}.
     *
     * <p>Reason: Bucket filtering and sharding are different logical mechanisms for selecting
     * subsets of table data. Applying both methods simultaneously introduces conflicting selection
     * criteria.
     */
    ReadBuilder withBucketFilter(Filter<Integer> bucketFilter);

    /**
     * Apply projection to the reader.
     *
     * <p>todo: update it.
     */
    default ReadBuilder withProjection(int[] projection) {
        if (projection == null) {
            return this;
        }
        int[][] nestedProjection =
                Arrays.stream(projection).mapToObj(i -> new int[] {i}).toArray(int[][]::new);
        return withProjection(nestedProjection);
    }

    /**
     * Push nested projection. For example, {@code [[0, 2, 1], ...]} specifies to include the 2nd
     * field of the 3rd field of the 1st field in the top-level row.
     */
    ReadBuilder withProjection(int[][] projection);

    /** the row number pushed down. */
    ReadBuilder withLimit(int limit);

    /**
     * Specify the shard to be read, and allocate sharded files to read records. Note that this
     * method cannot be used simultaneously with {@link #withBucketFilter(Filter)}.
     *
     * <p>Reason: Sharding and bucket filtering are different logical mechanisms for selecting
     * subsets of table data. Applying both methods simultaneously introduces conflicting selection
     * criteria.
     */
    ReadBuilder withShard(int indexOfThisSubtask, int numberOfParallelSubtasks);

    /** Create a {@link TableScan} to perform batch planning. */
    TableScan newScan();

    /** Create a {@link TableScan} to perform streaming planning. */
    StreamTableScan newStreamScan();

    /** Create a {@link TableRead} to read {@link Split}s. */
    TableRead newRead();
}
