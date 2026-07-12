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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.LongPredicate;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** ANN plus exact data-file fallback search for one snapshot bucket. */
public class PrimaryKeyVectorBucketSearch {

    private static final Comparator<PkVectorSearchResult> BEST_FIRST =
            Comparator.comparingDouble(PkVectorSearchResult::distance)
                    .thenComparing(PkVectorSearchResult::dataFileName)
                    .thenComparingLong(PkVectorSearchResult::rowPosition);

    private final PkVectorDataFileReader.Factory vectorReaderFactory;
    @Nullable private final PkVectorAnnSegmentSearcher annSearcher;
    private final Map<String, String> searchOptions;
    private final String metric;

    public PrimaryKeyVectorBucketSearch(
            PkVectorDataFileReader.Factory vectorReaderFactory,
            @Nullable PkVectorAnnSegmentSearcher annSearcher,
            Map<String, String> searchOptions,
            String metric) {
        this.vectorReaderFactory = vectorReaderFactory;
        this.annSearcher = annSearcher;
        this.searchOptions = Collections.unmodifiableMap(new HashMap<>(searchOptions));
        this.metric = metric;
    }

    public List<PkVectorSearchResult> search(
            PkVectorBucketIndexState state,
            List<DataFileMeta> activeFiles,
            Map<String, DeletionVector> deletionVectors,
            float[] query,
            int limit)
            throws IOException {
        checkArgument(limit > 0, "Vector search limit must be positive.");
        Map<String, DataFileMeta> filesByName = new HashMap<>();
        for (DataFileMeta file : activeFiles) {
            checkArgument(filesByName.put(file.fileName(), file) == null, "Duplicate data file.");
        }
        PriorityQueue<PkVectorSearchResult> nearest =
                new PriorityQueue<>(limit, BEST_FIRST.reversed());
        Set<String> covered = new HashSet<>();
        for (IndexFileMeta ann : state.annSegments()) {
            PkVectorSourceMeta sourceMeta = PkVectorSourceMeta.fromIndexFile(ann);
            for (PkVectorSourceFile source : sourceMeta.sourceFiles()) {
                DataFileMeta file = filesByName.get(source.fileName());
                checkArgument(
                        file != null && file.rowCount() == source.rowCount(),
                        "ANN source %s does not match the active data file.",
                        source.fileName());
                covered.add(source.fileName());
            }
            checkArgument(annSearcher != null, "ANN search is not configured.");
            for (PkVectorSearchResult result :
                    annSearcher.search(
                            ann, sourceMeta, query, limit, deletionVectors, searchOptions)) {
                add(nearest, result, limit);
            }
        }

        for (DataFileMeta file : activeFiles) {
            if (covered.contains(file.fileName())) {
                continue;
            }
            DeletionVector dv = deletionVectors.get(file.fileName());
            LongPredicate excluded = dv == null ? position -> false : dv::isDeleted;
            try (PkVectorReader reader = vectorReaderFactory.create(file)) {
                for (PkVectorSearchResult result :
                        PkVectorExactSearcher.search(
                                file.fileName(), reader, query, metric, limit, excluded)) {
                    add(nearest, result, limit);
                }
            }
        }
        List<PkVectorSearchResult> result = new ArrayList<>(nearest);
        Collections.sort(result, BEST_FIRST);
        return Collections.unmodifiableList(result);
    }

    private static void add(
            PriorityQueue<PkVectorSearchResult> nearest,
            PkVectorSearchResult candidate,
            int limit) {
        if (nearest.size() < limit) {
            nearest.add(candidate);
        } else if (BEST_FIRST.compare(candidate, nearest.peek()) < 0) {
            nearest.poll();
            nearest.add(candidate);
        }
    }
}
