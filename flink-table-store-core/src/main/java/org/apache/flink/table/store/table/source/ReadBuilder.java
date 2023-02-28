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

package org.apache.flink.table.store.table.source;

import org.apache.flink.table.store.annotation.Experimental;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.types.RowType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

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
 * <p>NOTE: {@link InternalRow} cannot be saved in memory. It may be reused internally, so you need
 * to convert it into your own data structure or copy it.
 *
 * @since 0.4.0
 */
@Experimental
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

    /**
     * Apply projection to the reader.
     *
     * <p>NOTE: Nested row projection is currently not supported.
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

    /** Create a {@link TableScan} to generate {@link Split}s. */
    TableScan newScan();

    /** Create a {@link TableRead} to read {@link Split}s. */
    TableRead newRead();
}
