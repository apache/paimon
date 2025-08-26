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

import org.apache.paimon.predicate.CompareUtils;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_FIRST;
import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_LAST;
import static org.apache.paimon.predicate.SortValue.SortDirection.ASCENDING;
import static org.apache.paimon.predicate.SortValue.SortDirection.DESCENDING;

/** Evaluate DataSplit TopN result. */
public class TopNDataSplitEvaluator {

    private final TableSchema schema;
    private final SchemaManager schemaManager;

    public TopNDataSplitEvaluator(TableSchema schema, SchemaManager schemaManager) {
        this.schema = schema;
        this.schemaManager = schemaManager;
    }

    public List<DataSplit> evaluate(TopN topN, List<DataSplit> splits) {
        // todo: we can support all the sort columns.
        List<SortValue> orders = topN.orders();
        if (orders.size() != 1) {
            return splits;
        }

        int limit = topN.limit();
        if (limit >= splits.size()) {
            return splits;
        }

        SortValue order = orders.get(0);
        return getTopNSplits(order, limit, splits);
    }

    private List<DataSplit> getTopNSplits(SortValue order, int limit, List<DataSplit> splits) {
        FieldRef ref = order.field();
        SortValue.SortDirection direction = order.direction();
        SortValue.NullOrdering nullOrdering = order.nullOrdering();

        int index = ref.index();
        DataField field = schema.fields().get(index);
        SimpleStatsEvolutions evolutions =
                new SimpleStatsEvolutions((id) -> schemaManager.schema(id).fields(), schema.id());

        // extract the stats
        List<DataSplit> results = new ArrayList<>();
        List<Pair<Stats, DataSplit>> pairs = new ArrayList<>();
        for (DataSplit split : splits) {
            if (!split.rawConvertible()) {
                return splits;
            }

            Set<String> cols = Collections.singleton(field.name());
            if (!split.statsAvailable(cols)) {
                results.add(split);
                continue;
            }

            Object min = split.minValue(index, field, evolutions);
            Object max = split.maxValue(index, field, evolutions);
            Long nullCount = split.nullCount(index, evolutions);
            Stats stats = new Stats(min, max, nullCount);
            pairs.add(Pair.of(stats, split));
        }

        // pick the TopN splits
        if (NULLS_FIRST.equals(nullOrdering)) {
            results.addAll(pickNullFirstSplits(pairs, ref, direction, limit));
        } else if (NULLS_LAST.equals(nullOrdering)) {
            results.addAll(pickNullLastSplits(pairs, ref, direction, limit));
        } else {
            return splits;
        }

        return results;
    }

    private List<DataSplit> pickNullFirstSplits(
            List<Pair<Stats, DataSplit>> pairs,
            FieldRef field,
            SortValue.SortDirection direction,
            int limit) {
        Comparator<Pair<Stats, DataSplit>> comparator;
        if (ASCENDING.equals(direction)) {
            comparator =
                    (x, y) -> {
                        Stats left = x.getKey();
                        Stats right = y.getKey();
                        int result = nullsFirst(left.nullCount, right.nullCount);
                        if (result == 0) {
                            result = asc(field, left.min, right.min);
                        }
                        return result;
                    };
        } else if (DESCENDING.equals(direction)) {
            comparator =
                    (x, y) -> {
                        Stats left = x.getKey();
                        Stats right = y.getKey();
                        int result = nullsFirst(left.nullCount, right.nullCount);
                        if (result == 0) {
                            result = desc(field, left.max, right.max);
                        }
                        return result;
                    };
        } else {
            return pairs.stream().map(Pair::getValue).collect(Collectors.toList());
        }
        pairs.sort(comparator);

        long scanned = 0;
        List<DataSplit> splits = new ArrayList<>();
        for (Pair<Stats, DataSplit> pair : pairs) {
            Stats stats = pair.getKey();
            DataSplit split = pair.getValue();
            splits.add(split);
            scanned += Math.max(stats.nullCount, 1);
            if (scanned >= limit) {
                break;
            }
        }
        return splits;
    }

    private List<DataSplit> pickNullLastSplits(
            List<Pair<Stats, DataSplit>> pairs,
            FieldRef field,
            SortValue.SortDirection direction,
            int limit) {
        Comparator<Pair<Stats, DataSplit>> comparator;
        if (ASCENDING.equals(direction)) {
            comparator =
                    (x, y) -> {
                        Stats left = x.getKey();
                        Stats right = y.getKey();
                        int result = asc(field, left.min, right.min);
                        if (result == 0) {
                            result = nullsLast(left.nullCount, right.nullCount);
                        }
                        return result;
                    };
        } else if (DESCENDING.equals(direction)) {
            comparator =
                    (x, y) -> {
                        Stats left = x.getKey();
                        Stats right = y.getKey();
                        int result = desc(field, left.max, right.max);
                        if (result == 0) {
                            result = nullsLast(left.nullCount, right.nullCount);
                        }
                        return result;
                    };
        } else {
            return pairs.stream().map(Pair::getValue).collect(Collectors.toList());
        }

        return pairs.stream()
                .sorted(comparator)
                .map(Pair::getValue)
                .limit(limit)
                .collect(Collectors.toList());
    }

    private int nullsFirst(Long left, Long right) {
        if (left == null) {
            return -1;
        } else if (right == null) {
            return 1;
        } else {
            return -Long.compare(left, right);
        }
    }

    private int nullsLast(Long left, Long right) {
        if (left == null) {
            return -1;
        } else if (right == null) {
            return 1;
        } else {
            return Long.compare(left, right);
        }
    }

    private int asc(FieldRef field, Object left, Object right) {
        if (left == null) {
            return -1;
        } else if (right == null) {
            return 1;
        } else {
            return CompareUtils.compareLiteral(field.type(), left, right);
        }
    }

    private int desc(FieldRef field, Object left, Object right) {
        if (left == null) {
            return -1;
        } else if (right == null) {
            return 1;
        } else {
            return -CompareUtils.compareLiteral(field.type(), left, right);
        }
    }

    /** The DataSplit's stats. */
    private static class Stats {
        Object min;
        Object max;
        Long nullCount;

        public Stats(Object min, Object max, Long nullCount) {
            this.min = min;
            this.max = max;
            this.nullCount = nullCount;
        }
    }
}
