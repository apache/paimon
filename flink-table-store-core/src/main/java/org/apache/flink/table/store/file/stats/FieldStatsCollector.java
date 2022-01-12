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

package org.apache.flink.table.store.file.stats;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

/** Collector to extract statistics of each fields from a series of records. */
public class FieldStatsCollector {

    private final Object[] minValues;
    private final Object[] maxValues;
    private final long[] nullCounts;

    private final RowData.FieldGetter[] fieldGetters;

    public FieldStatsCollector(RowType rowType) {
        int numFields = rowType.getFieldCount();
        this.minValues = new Object[numFields];
        this.maxValues = new Object[numFields];
        this.nullCounts = new long[numFields];
        this.fieldGetters = FieldStatsArraySerializer.createFieldGetters(rowType);
    }

    /**
     * Update the statistics with a new row data.
     *
     * <p><b>IMPORTANT</b>: Fields of this row should NOT be reused, as they're directly stored in
     * the collector.
     */
    public void collect(RowData row) {
        Preconditions.checkArgument(
                fieldGetters.length == row.getArity(),
                "Expecting row data with %d fields but found row data with %d fields",
                fieldGetters.length,
                row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
            Object obj = fieldGetters[i].getFieldOrNull(row);
            if (obj == null) {
                nullCounts[i]++;
                continue;
            }

            // TODO use comparator for not comparable types and extract this logic to a util class
            if (!(obj instanceof Comparable)) {
                continue;
            }
            Comparable<Object> c = (Comparable<Object>) obj;
            if (minValues[i] == null || c.compareTo(minValues[i]) < 0) {
                minValues[i] = c;
            }
            if (maxValues[i] == null || c.compareTo(maxValues[i]) > 0) {
                maxValues[i] = c;
            }
        }
    }

    public FieldStats[] extract() {
        FieldStats[] stats = new FieldStats[fieldGetters.length];
        for (int i = 0; i < stats.length; i++) {
            stats[i] = new FieldStats(minValues[i], maxValues[i], nullCounts[i]);
        }
        return stats;
    }
}
