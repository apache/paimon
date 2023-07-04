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

package org.apache.paimon.format;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Collector to extract statistics of each fields from a series of records. */
public class TableStatsCollector {

    private final RowDataToObjectArrayConverter converter;
    private final FieldStatsCollector[] statsCollectors;
    private final Serializer<Object>[] fieldSerializers;

    public TableStatsCollector(RowType rowType, FieldStatsCollector.Factory[] collectorFactory) {
        int numFields = rowType.getFieldCount();
        checkArgument(
                numFields == collectorFactory.length,
                "numFields %s should equal to stats length %s.",
                numFields,
                collectorFactory.length);
        this.statsCollectors = FieldStatsCollector.create(collectorFactory);
        this.converter = new RowDataToObjectArrayConverter(rowType);
        this.fieldSerializers = new Serializer[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldSerializers[i] = InternalSerializers.create(rowType.getTypeAt(i));
        }
    }

    /**
     * Update the statistics with a new row data.
     *
     * <p><b>IMPORTANT</b>: Fields of this row should NOT be reused, as they're directly stored in
     * the collector.
     */
    public void collect(InternalRow row) {
        Object[] objects = converter.convert(row);
        for (int i = 0; i < row.getFieldCount(); i++) {
            FieldStatsCollector collector = statsCollectors[i];
            Object obj = objects[i];
            collector.collect(obj, fieldSerializers[i]);
        }
    }

    public FieldStats[] extract() {
        FieldStats[] stats = new FieldStats[this.statsCollectors.length];
        for (int i = 0; i < stats.length; i++) {
            stats[i] = this.statsCollectors[i].result();
        }
        return stats;
    }
}
