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

package org.apache.paimon.operation;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.ProjectedManifestEntry;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.ThrowingConsumer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * Sorts valid binary entries with bounded buffers and decodes one complete RowID group at a time.
 */
final class DataEvolutionManifestEntryProducer implements AutoCloseable {

    private static final RowType SORT_TYPE =
            DataTypes.ROW(
                    DataTypes.BIGINT(),
                    DataTypes.BIGINT(),
                    DataTypes.BIGINT(),
                    ManifestEntry.MANIFEST_ROW_TYPE);

    private final ManifestEntryExternalSort.BinaryRowSorter sorter;
    private final GenericRow row = new GenericRow(4);
    private long ordinal;

    DataEvolutionManifestEntryProducer(ManifestEntryExternalSort.ExternalSortConfig config) {
        sorter =
                new ManifestEntryExternalSort.BinaryRowSorter(
                        SORT_TYPE, new int[] {0, 1, 2}, config);
    }

    void write(ProjectedManifestEntry entry) throws Exception {
        Long first = entry.file().firstRowId();
        long count = entry.file().rowCount();
        if (first == null || count <= 0 || first > Long.MAX_VALUE - (count - 1)) {
            throw new UnsupportedOperationException(
                    "Incremental DE planning requires valid non-null row id ranges.");
        }
        row.setField(0, first);
        row.setField(1, first + count - 1);
        row.setField(2, ordinal);
        row.setField(3, entry.fullRow());
        try {
            sorter.write(row);
            ordinal = Math.incrementExact(ordinal);
        } finally {
            row.setField(3, null);
        }
    }

    void forEachGroup(ThrowingConsumer<List<ManifestEntry>, Exception> consumer) throws Exception {
        forEachGroup(consumer, () -> true);
    }

    /** Stops only between complete dependency groups, without decoding a partial successor. */
    void forEachGroup(
            ThrowingConsumer<List<ManifestEntry>, Exception> consumer,
            BooleanSupplier continueReading)
            throws Exception {
        MutableObjectIterator<BinaryRow> iterator = sorter.sortedIterator();
        BinaryRow reuse = new BinaryRow(SORT_TYPE.getFieldCount());
        ManifestEntrySerializer serializer = new ManifestEntrySerializer();
        List<OrderedEntry> group = new ArrayList<>();
        long end = Long.MIN_VALUE;
        BinaryRow next;
        while ((next = iterator.next(reuse)) != null) {
            long first = next.getLong(0);
            if (!group.isEmpty() && first > end) {
                emit(group, consumer);
                if (!continueReading.getAsBoolean()) {
                    return;
                }
                group.clear();
            }
            end = group.isEmpty() ? next.getLong(1) : Math.max(end, next.getLong(1));
            // fromRow creates independent metadata before the sort iterator reuses its row.
            group.add(
                    new OrderedEntry(
                            next.getLong(2),
                            serializer.fromRow(
                                    next.getRow(
                                            3, ManifestEntry.MANIFEST_ROW_TYPE.getFieldCount()))));
        }
        if (!group.isEmpty()) {
            emit(group, consumer);
        }
    }

    private static void emit(
            List<OrderedEntry> group, ThrowingConsumer<List<ManifestEntry>, Exception> consumer)
            throws Exception {
        // Preserve RangeHelper's original input order, including equal-version providers.
        group.sort(Comparator.comparingLong(entry -> entry.ordinal));
        List<ManifestEntry> entries = new ArrayList<>(group.size());
        for (OrderedEntry entry : group) {
            entries.add(entry.entry);
        }
        consumer.accept(entries);
    }

    @Override
    public void close() throws Exception {
        sorter.close();
    }

    private static final class OrderedEntry {
        private final long ordinal;
        private final ManifestEntry entry;

        private OrderedEntry(long ordinal, ManifestEntry entry) {
            this.ordinal = ordinal;
            this.entry = entry;
        }
    }
}
