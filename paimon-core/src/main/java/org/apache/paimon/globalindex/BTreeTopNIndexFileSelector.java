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

/**
 * Selects BTree index files which may contain a single-column descending TopN result.
 *
 * <p>Files without usable sorted metadata are always retained. For files with usable metadata,
 * retaining the first {@code N} files ordered by their best value is safe because every non-empty
 * BTree file contributes at least one row at that value.
 */
class BTreeTopNIndexFileSelector {

    private final KeySerializer keySerializer;
    private final Comparator<Object> keyComparator;
    private final boolean nullsFirst;

    private BTreeTopNIndexFileSelector(DataField field, TopN topN) {
        this.keySerializer = KeySerializer.create(field.type());
        this.keyComparator = keySerializer.createComparator();
        this.nullsFirst = topN.orders().get(0).nullOrdering() == SortValue.NullOrdering.NULLS_FIRST;
    }

    static List<IndexFileMeta> select(List<IndexFileMeta> files, DataField field, TopN topN) {
        int limit = topN.limit();
        if (limit == 0) {
            return new ArrayList<>();
        }
        if (limit >= files.size()) {
            return new ArrayList<>(files);
        }

        BTreeTopNIndexFileSelector selector = new BTreeTopNIndexFileSelector(field, topN);
        List<IndexFileMeta> selected = new ArrayList<>();
        List<RankedIndexFile> rankedFiles = new ArrayList<>();
        for (IndexFileMeta file : files) {
            RankedIndexFile rankedFile = selector.tryRank(file);
            if (rankedFile == null) {
                // Match TopNDataSplitEvaluator: unknown sources cannot be pruned.
                selected.add(file);
            } else {
                rankedFiles.add(rankedFile);
            }
        }

        rankedFiles.sort(selector::compare);
        for (int i = 0; i < Math.min(limit, rankedFiles.size()); i++) {
            selected.add(rankedFiles.get(i).file);
        }
        return selected;
    }

    @Nullable
    private RankedIndexFile tryRank(IndexFileMeta file) {
        GlobalIndexMeta globalIndex = file.globalIndexMeta();
        if (file.rowCount() <= 0 || globalIndex == null || globalIndex.indexMeta() == null) {
            return null;
        }

        try {
            SortedIndexFileMeta sortedMeta =
                    SortedIndexFileMeta.deserialize(globalIndex.indexMeta());
            byte[] lastKey = sortedMeta.lastKey();
            if (lastKey == null && !sortedMeta.hasNulls()) {
                return null;
            }

            boolean bestIsNull = nullsFirst ? sortedMeta.hasNulls() : lastKey == null;
            Object bestKey =
                    bestIsNull ? null : keySerializer.deserialize(MemorySlice.wrap(lastKey));
            return new RankedIndexFile(file, bestIsNull, bestKey);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private int compare(RankedIndexFile left, RankedIndexFile right) {
        if (left.bestIsNull != right.bestIsNull) {
            if (left.bestIsNull) {
                return nullsFirst ? -1 : 1;
            }
            return nullsFirst ? 1 : -1;
        }

        if (!left.bestIsNull) {
            int result = keyComparator.compare(right.bestKey, left.bestKey);
            if (result != 0) {
                return result;
            }
        }
        return left.file.fileName().compareTo(right.file.fileName());
    }

    private static class RankedIndexFile {

        private final IndexFileMeta file;
        private final boolean bestIsNull;
        @Nullable private final Object bestKey;

        private RankedIndexFile(IndexFileMeta file, boolean bestIsNull, @Nullable Object bestKey) {
            this.file = file;
            this.bestIsNull = bestIsNull;
            this.bestKey = bestKey;
        }
    }
}
