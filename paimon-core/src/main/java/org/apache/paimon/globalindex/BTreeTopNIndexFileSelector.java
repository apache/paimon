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

package org.apache.paimon.globalindex;

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.types.DataField;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Selects BTree index files which may contain a single-column TopN result.
 *
 * <p>Every BTree file must have sorted metadata, matching the predicate reader contract. Retaining
 * the first {@code N} files ordered by their best value is safe because every BTree file
 * contributes at least one row at that value. Fewer files can be retained when one file alone
 * contains at least {@code N} rows and its worst value is not worse than the best value of every
 * remaining file.
 */
class BTreeTopNIndexFileSelector {

    private final KeySerializer keySerializer;
    private final Comparator<Object> keyComparator;
    private final boolean ascending;
    private final boolean nullsFirst;

    private BTreeTopNIndexFileSelector(DataField field, TopN topN) {
        this.keySerializer = KeySerializer.create(field.type());
        this.keyComparator = keySerializer.createComparator();
        this.ascending = topN.orders().get(0).direction() == SortValue.SortDirection.ASCENDING;
        this.nullsFirst = topN.orders().get(0).nullOrdering() == SortValue.NullOrdering.NULLS_FIRST;
    }

    static List<IndexFileMeta> select(List<IndexFileMeta> files, DataField field, TopN topN) {
        int limit = topN.limit();
        if (limit == 0) {
            return new ArrayList<>();
        }

        BTreeTopNIndexFileSelector selector = new BTreeTopNIndexFileSelector(field, topN);
        List<IndexFileMeta> selected = new ArrayList<>();
        List<RankedIndexFile> rankedFiles = new ArrayList<>();
        for (IndexFileMeta file : files) {
            rankedFiles.add(selector.rank(file));
        }

        rankedFiles.sort(selector::compare);
        for (int i = 0; i < Math.min(limit, rankedFiles.size()); i++) {
            RankedIndexFile current = rankedFiles.get(i);
            selected.add(current.file);
            if (i + 1 < rankedFiles.size()
                    && current.file.rowCount() >= limit
                    // Equal boundary keys are safe because this TopN has no secondary ordering or
                    // WITH TIES semantics, and the current file alone supplies enough rows.
                    && selector.compareWorstToBest(current, rankedFiles.get(i + 1)) <= 0) {
                break;
            }
        }
        return selected;
    }

    private RankedIndexFile rank(IndexFileMeta file) {
        GlobalIndexMeta globalIndex =
                checkNotNull(
                        file.globalIndexMeta(),
                        "BTree index file '%s' is missing global index metadata.",
                        file.fileName());
        byte[] indexMeta =
                checkNotNull(
                        globalIndex.indexMeta(),
                        "BTree index file '%s' is missing sorted metadata.",
                        file.fileName());
        SortedIndexFileMeta sortedMeta = SortedIndexFileMeta.deserialize(indexMeta);
        byte[] firstKey = sortedMeta.firstKey();
        byte[] lastKey = sortedMeta.lastKey();
        boolean hasNonNulls = lastKey != null;

        byte[] nonNullBestKey = ascending ? firstKey : lastKey;
        byte[] nonNullWorstKey = ascending ? lastKey : firstKey;
        boolean bestIsNull = nullsFirst ? sortedMeta.hasNulls() : !hasNonNulls;
        Object bestKey =
                bestIsNull ? null : keySerializer.deserialize(MemorySlice.wrap(nonNullBestKey));
        boolean worstIsNull = nullsFirst ? !hasNonNulls : sortedMeta.hasNulls();
        Object worstKey =
                worstIsNull ? null : keySerializer.deserialize(MemorySlice.wrap(nonNullWorstKey));
        return new RankedIndexFile(file, bestIsNull, bestKey, worstIsNull, worstKey);
    }

    private int compare(RankedIndexFile left, RankedIndexFile right) {
        int result = compareValues(left.bestIsNull, left.bestKey, right.bestIsNull, right.bestKey);
        if (result != 0) {
            return result;
        }
        return left.file.fileName().compareTo(right.file.fileName());
    }

    private int compareWorstToBest(RankedIndexFile current, RankedIndexFile next) {
        return compareValues(current.worstIsNull, current.worstKey, next.bestIsNull, next.bestKey);
    }

    private int compareValues(
            boolean leftIsNull,
            @Nullable Object left,
            boolean rightIsNull,
            @Nullable Object right) {
        if (leftIsNull != rightIsNull) {
            if (leftIsNull) {
                return nullsFirst ? -1 : 1;
            }
            return nullsFirst ? 1 : -1;
        }
        if (leftIsNull) {
            return 0;
        }
        return ascending ? keyComparator.compare(left, right) : keyComparator.compare(right, left);
    }

    private static class RankedIndexFile {

        private final IndexFileMeta file;
        private final boolean bestIsNull;
        @Nullable private final Object bestKey;
        private final boolean worstIsNull;
        @Nullable private final Object worstKey;

        private RankedIndexFile(
                IndexFileMeta file,
                boolean bestIsNull,
                @Nullable Object bestKey,
                boolean worstIsNull,
                @Nullable Object worstKey) {
            this.file = file;
            this.bestIsNull = bestIsNull;
            this.bestKey = bestKey;
            this.worstIsNull = worstIsNull;
            this.worstKey = worstKey;
        }
    }
}
