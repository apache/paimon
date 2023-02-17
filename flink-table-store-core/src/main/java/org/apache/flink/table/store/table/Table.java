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

package org.apache.flink.table.store.table;

import org.apache.flink.table.store.annotation.Experimental;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.TableScan;
import org.apache.flink.table.store.types.RowType;

import java.io.Serializable;
import java.util.Map;

/**
 * A table provides basic abstraction for table type and table scan and table read.
 *
 * <p>Example of distributed reading a table:
 *
 * <pre>{@code
 * // 1. Plan splits in 'Coordinator' (or named 'Driver'):
 * Table table = catalog.getTable(...);
 * List<Split> splits = table.newScan()
 *     .withFilter(...)
 *     .plan()
 *     .splits();
 *
 * // 2. Distribute these splits to different tasks
 *
 * // 3. Read a split in task
 * TableRead read = table.newRead().withProjection();
 * RecordReader<InternalRow> reader = read.createReader(split);
 * reader.forEachRemaining(...);
 *
 * }</pre>
 *
 * <p>Example of single node reading:
 *
 * <pre>{@code
 * Table table = catalog.getTable(...);
 * List<Split> splits = table.newScan()
 *     .withFilter(predicates)
 *     .plan()
 *     .splits();
 * RecordReader<InternalRow> reader = table.newRead()
 *     .withFilter(...)
 *     .withProjection(...)
 *     .createReader(splits);
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
public interface Table extends Serializable {

    String name();

    RowType rowType();

    /** Create a {@link TableScan} to generate {@link Split}s. */
    TableScan newScan();

    /** Create a {@link TableRead} to read {@link Split}s. */
    TableRead newRead();

    /** Copy this table with adding dynamic options. */
    Table copy(Map<String, String> dynamicOptions);
}
