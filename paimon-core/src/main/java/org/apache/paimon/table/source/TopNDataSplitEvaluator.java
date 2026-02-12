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
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

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
            if (!minmaxAvailable(split, Collections.singleton(field.name()))) {
                // unknown split, read it
                results.add(split);
                continue;
            }

            DataSplit dataSplit = (DataSplit) split;
            Object min = dataSplit.minValue(index, field, evolutions);
            Object max = dataSplit.maxValue(index, field, evolutions);
            Long nullCount = dataSplit.nullCount(index, evolutions);
            richSplits.add(new RichSplit(dataSplit, min, max, nullCount));
        }

        // pick the TopN splits
        boolean nullFirst = NULLS_FIRST.equals(order.nullOrdering());
        boolean ascending = ASCENDING.equals(order.direction());
        results.addAll(pickTopNSplits(richSplits, field.type(), ascending, nullFirst, limit));
        return results;
    }

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
                            result = ascCompare(fieldType, x.min, y.min);
                            if (result == 0) {
                                result = nullsLastCompare(x.nullCount, y.nullCount);
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
                            result = descCompare(fieldType, x.max, y.max);
                            if (result == 0) {
                                result = nullsLastCompare(x.nullCount, y.nullCount);
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
            return -1;
        } else if (right == null) {
            return 1;
        } else {
            return -Long.compare(left, right);
        }
    }

    private int nullsLastCompare(Long left, Long right) {
        if (left == null) {
            return -1;
        } else if (right == null) {
            return 1;
        } else {
            return Long.compare(left, right);
        }
    }

    private int ascCompare(DataType type, Object left, Object right) {
        if (left == null) {
            return -1;
        } else if (right == null) {
            return 1;
        } else {
            return CompareUtils.compareLiteral(type, left, right);
        }
    }

    private int descCompare(DataType type, Object left, Object right) {
        if (left == null) {
            return -1;
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

        private RichSplit(DataSplit split, Object min, Object max, Long nullCount) {
            this.split = split;
            this.min = min;
            this.max = max;
            this.nullCount = nullCount;
        }

        private DataSplit split() {
            return split;
        }
    }
}
