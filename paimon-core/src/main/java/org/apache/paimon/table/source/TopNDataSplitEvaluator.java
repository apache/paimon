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

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.CompareUtils;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InternalRowUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_FIRST;
import static org.apache.paimon.predicate.SortValue.SortDirection.ASCENDING;
import static org.apache.paimon.table.source.PushDownUtils.minmaxAvailable;
import static org.apache.paimon.table.source.PushDownUtils.tightBoundsAvailable;

/** Evaluate DataSplit TopN result. */
public class TopNDataSplitEvaluator {

    private final Map<Long, TableSchema> tableSchemas;
    private final TableSchema schema;
    private final SchemaManager schemaManager;

    public TopNDataSplitEvaluator(TableSchema schema, SchemaManager schemaManager) {
        this.tableSchemas = new HashMap<>();
        this.schema = schema;
        this.schemaManager = schemaManager;
    }

    public List<Split> evaluate(SortValue order, int limit, List<Split> splits) {
        if (limit > splits.size()) {
            return splits;
        }
        return getTopNSplits(order, limit, splits);
    }

    private List<Split> getTopNSplits(SortValue order, int limit, List<Split> splits) {
        int index = order.field().index();
        DataField field = schema.fields().get(index);
        SimpleStatsEvolutions evolutions =
                new SimpleStatsEvolutions((id) -> scanTableSchema(id).fields(), schema.id());

        // extract the stats
        List<Split> results = new ArrayList<>();
        List<RichSplit> richSplits = new ArrayList<>();
        for (Split split : splits) {
            if (!minmaxAvailable(split, Collections.singleton(field.name()))
                    || !tightBoundsAvailable(split)) {
                // unknown split, read it
                results.add(split);
                continue;
            }

            DataSplit dataSplit = (DataSplit) split;
            SplitStats stats = extractStats(dataSplit, index, field, evolutions);
            if (!stats.usableForPruning()) {
                // Some file of the split lacks min/max/null-count statistics for the sort
                // column (stats.mode=counts/none, or files written before the column was
                // added). The aggregate bound is then not a true bound, so ordering the
                // split against the others could drop a split holding a top row — read it.
                results.add(dataSplit);
                continue;
            }
            richSplits.add(new RichSplit(dataSplit, stats.min, stats.max, stats.nullCount));
        }

        // pick the TopN splits
        boolean nullFirst = NULLS_FIRST.equals(order.nullOrdering());
        boolean ascending = ASCENDING.equals(order.direction());
        results.addAll(pickTopNSplits(richSplits, field.type(), ascending, nullFirst, limit));
        return results;
    }

    /**
     * Aggregated sort-column statistics of a split, computed per file so that missing statistics of
     * ANY file are visible. {@link DataSplit#minValue} and friends aggregate by skipping files
     * without statistics, which produces a value that looks known but is not a true bound once a
     * single file is unknown.
     */
    private static class SplitStats {

        private final Object min;
        private final Object max;
        private final Long nullCount;
        private final boolean complete;

        private SplitStats(Object min, Object max, Long nullCount, boolean complete) {
            this.min = min;
            this.max = max;
            this.nullCount = nullCount;
            this.complete = complete;
        }

        /** Whether the split can safely participate in ordering-based pruning. */
        private boolean usableForPruning() {
            return complete;
        }
    }

    private SplitStats extractStats(
            DataSplit split, int fieldIndex, DataField field, SimpleStatsEvolutions evolutions) {
        Object min = null;
        Object max = null;
        Long nullCount = null;
        boolean complete = true;
        for (DataFileMeta file : split.dataFiles()) {
            SimpleStatsEvolution evolution = evolutions.getOrCreate(file.schemaId());
            Long fileNullCount =
                    (Long)
                            InternalRowUtils.get(
                                    evolution.evolution(
                                            file.valueStats().nullCounts(),
                                            file.rowCount(),
                                            file.valueStatsCols()),
                                    fieldIndex,
                                    DataTypes.BIGINT());

            if (fileNullCount != null && fileNullCount.longValue() == file.rowCount()) {
                // provably no non-null value in this file: it legitimately contributes
                // nothing to min/max (all-null column, or file written before the column
                // was added)
                nullCount = nullCount == null ? fileNullCount : nullCount + fileNullCount;
                continue;
            }

            Object fileMin =
                    InternalRowUtils.get(
                            evolution.evolution(
                                    file.valueStats().minValues(), file.valueStatsCols()),
                            fieldIndex,
                            field.type());
            Object fileMax =
                    InternalRowUtils.get(
                            evolution.evolution(
                                    file.valueStats().maxValues(), file.valueStatsCols()),
                            fieldIndex,
                            field.type());
            if (fileMin == null || fileMax == null || fileNullCount == null) {
                // statistics not collected for this file (stats.mode=counts/none): the
                // aggregate bound would not be a true bound
                complete = false;
                continue;
            }
            nullCount = nullCount == null ? fileNullCount : nullCount + fileNullCount;
            if (min == null || CompareUtils.compareLiteral(field.type(), fileMin, min) < 0) {
                min = fileMin;
            }
            if (max == null || CompareUtils.compareLiteral(field.type(), fileMax, max) > 0) {
                max = fileMax;
            }
        }
        return new SplitStats(min, max, nullCount, complete);
    }

    /**
     * Orders splits by their best row under the query's sort order and keeps the first {@code
     * limit} ones. In the NULLS LAST branches a split whose sort column is provably all null
     * ({@link RichSplit#allNull}, i.e. the null count equals the row count) is the worst candidate
     * and sorts last. A null min/max that is not provably all-null means the bound is unknown — for
     * example files written with {@code stats.mode=counts} — and {@link #ascCompare}/{@link
     * #descCompare} order such a split first so it is read, conservatively.
     */
    private List<DataSplit> pickTopNSplits(
            List<RichSplit> splits,
            DataType fieldType,
            boolean ascending,
            boolean nullFirst,
            int limit) {
        Comparator<RichSplit> comparator;
        if (ascending) {
            comparator =
                    (x, y) -> {
                        int result;
                        if (nullFirst) {
                            result = nullsFirstCompare(x.nullCount, y.nullCount);
                            if (result == 0) {
                                result = ascCompare(fieldType, x.min, y.min);
                            }
                        } else {
                            result = Boolean.compare(x.allNull, y.allNull);
                            if (result == 0) {
                                result = ascCompare(fieldType, x.min, y.min);
                                if (result == 0) {
                                    result = nullsLastCompare(x.nullCount, y.nullCount);
                                }
                            }
                        }
                        return result;
                    };
        } else {
            comparator =
                    (x, y) -> {
                        int result;
                        if (nullFirst) {
                            result = nullsFirstCompare(x.nullCount, y.nullCount);
                            if (result == 0) {
                                result = descCompare(fieldType, x.max, y.max);
                            }
                        } else {
                            result = Boolean.compare(x.allNull, y.allNull);
                            if (result == 0) {
                                result = descCompare(fieldType, x.max, y.max);
                                if (result == 0) {
                                    result = nullsLastCompare(x.nullCount, y.nullCount);
                                }
                            }
                        }
                        return result;
                    };
        }
        return splits.stream()
                .sorted(comparator)
                .map(RichSplit::split)
                .limit(limit)
                .collect(Collectors.toList());
    }

    private int nullsFirstCompare(Long left, Long right) {
        if (left == null) {
            return right == null ? 0 : -1;
        } else if (right == null) {
            return 1;
        } else {
            return -Long.compare(left, right);
        }
    }

    private int nullsLastCompare(Long left, Long right) {
        if (left == null) {
            return right == null ? 0 : -1;
        } else if (right == null) {
            return 1;
        } else {
            return Long.compare(left, right);
        }
    }

    private int ascCompare(DataType type, Object left, Object right) {
        if (left == null) {
            return right == null ? 0 : -1;
        } else if (right == null) {
            return 1;
        } else {
            return CompareUtils.compareLiteral(type, left, right);
        }
    }

    private int descCompare(DataType type, Object left, Object right) {
        if (left == null) {
            return right == null ? 0 : -1;
        } else if (right == null) {
            return 1;
        } else {
            return -CompareUtils.compareLiteral(type, left, right);
        }
    }

    private TableSchema scanTableSchema(long id) {
        return tableSchemas.computeIfAbsent(
                id, key -> key == schema.id() ? schema : schemaManager.schema(id));
    }

    /** DataSplit with stats. */
    private static class RichSplit {

        private final DataSplit split;
        private final Object min;
        private final Object max;
        private final Long nullCount;
        private final boolean allNull;

        private RichSplit(DataSplit split, Object min, Object max, Long nullCount) {
            this.split = split;
            this.min = min;
            this.max = max;
            this.nullCount = nullCount;
            this.allNull = nullCount != null && nullCount.longValue() == split.rowCount();
        }

        private DataSplit split() {
            return split;
        }
    }
}
