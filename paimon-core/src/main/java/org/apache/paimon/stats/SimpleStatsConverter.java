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

package org.apache.paimon.stats;

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Converter for array of {@link SimpleColStats}. */
public class SimpleStatsConverter {

    private final RowType rowType;
    private final boolean denseStore;
    private final InternalRowSerializer serializer;
    private final Map<List<String>, InternalRowSerializer> serializers;

    public SimpleStatsConverter(RowType type) {
        this(type, false);
    }

    public SimpleStatsConverter(RowType type, boolean denseStore) {
        // as stated in RollingFile.Writer#finish, col stats are not collected currently so
        // min/max values are all nulls
        this.rowType =
                type.copy(
                        type.getFields().stream()
                                .map(f -> f.newType(f.type().copy(true)))
                                .collect(Collectors.toList()));
        this.denseStore = denseStore;
        this.serializer = new InternalRowSerializer(rowType);
        this.serializers = new HashMap<>();
    }

    public Pair<List<String>, SimpleStats> toBinary(SimpleColStats[] stats) {
        return denseStore ? toBinaryDenseMode(stats) : Pair.of(null, toBinaryAllMode(stats));
    }

    private Pair<List<String>, SimpleStats> toBinaryDenseMode(SimpleColStats[] stats) {
        List<String> fields = new ArrayList<>();
        List<Object> minValues = new ArrayList<>();
        List<Object> maxValues = new ArrayList<>();
        List<Long> nullCounts = new ArrayList<>();
        for (int i = 0; i < stats.length; i++) {
            SimpleColStats colStats = stats[i];
            if (colStats.isNone()) {
                continue;
            }

            fields.add(rowType.getFields().get(i).name());
            minValues.add(colStats.min());
            maxValues.add(colStats.max());
            nullCounts.add(colStats.nullCount());
        }

        InternalRowSerializer serializer =
                serializers.computeIfAbsent(
                        fields, key -> new InternalRowSerializer(rowType.project(key)));

        SimpleStats simpleStats =
                new SimpleStats(
                        serializer.toBinaryRow(GenericRow.of(minValues.toArray())).copy(),
                        serializer.toBinaryRow(GenericRow.of(maxValues.toArray())).copy(),
                        BinaryArray.fromLongArray(nullCounts.toArray(new Long[0])));
        return Pair.of(fields.size() == rowType.getFieldCount() ? null : fields, simpleStats);
    }

    public SimpleStats toBinaryAllMode(SimpleColStats[] stats) {
        int rowFieldCount = stats.length;
        GenericRow minValues = new GenericRow(rowFieldCount);
        GenericRow maxValues = new GenericRow(rowFieldCount);
        Long[] nullCounts = new Long[rowFieldCount];
        for (int i = 0; i < rowFieldCount; i++) {
            minValues.setField(i, stats[i].min());
            maxValues.setField(i, stats[i].max());
            nullCounts[i] = stats[i].nullCount();
        }
        return new SimpleStats(
                serializer.toBinaryRow(minValues).copy(),
                serializer.toBinaryRow(maxValues).copy(),
                BinaryArray.fromLongArray(nullCounts));
    }
}
