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

import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.GlobalIndexReadThreadPool;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.pkvector.PkVectorAnnSegmentSearcher;
import org.apache.paimon.index.pkvector.PkVectorBucketIndexState;
import org.apache.paimon.index.pkvector.PkVectorDataFileReader;
import org.apache.paimon.index.pkvector.PkVectorSearchResult;
import org.apache.paimon.index.pkvector.PkVectorSourcePolicy;
import org.apache.paimon.index.pkvector.PrimaryKeyVectorBucketSearch;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.VectorType;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;
import static org.apache.paimon.globalindex.VectorSearchMetric.normalize;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Executes bucket-local primary-key vector search and merges physical candidates globally. */
public class PrimaryKeyVectorRead implements VectorRead, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Comparator<Candidate> BEST_FIRST =
            (left, right) -> {
                int distance = Float.compare(left.distance, right.distance);
                if (distance != 0) {
                    return distance;
                }
                int partition = compareBytes(left.partition.toBytes(), right.partition.toBytes());
                if (partition != 0) {
                    return partition;
                }
                int bucket = Integer.compare(left.bucket, right.bucket);
                if (bucket != 0) {
                    return bucket;
                }
                int fileName = left.dataFileName.compareTo(right.dataFileName);
                return fileName != 0 ? fileName : Long.compare(left.rowPosition, right.rowPosition);
            };

    protected final FileStoreTable table;
    protected final DataField vectorField;
    private final String indexType;
    private final Options indexOptions;
    private final Map<String, String> searchOptions;
    private final float[] query;
    protected final int limit;
    private final String metric;

    public PrimaryKeyVectorRead(
            FileStoreTable table,
            DataField vectorField,
            float[] query,
            int limit,
            Map<String, String> searchOptions) {
        checkArgument(
                vectorField.type() instanceof VectorType, "Vector field must use VECTOR type.");
        checkArgument(
                query.length == ((VectorType) vectorField.type()).getLength(),
                "Query dimension %s does not match vector field dimension %s.",
                query.length,
                ((VectorType) vectorField.type()).getLength());
        checkArgument(limit > 0, "Vector search limit must be positive: %s.", limit);
        this.table = table;
        this.vectorField = vectorField;
        this.indexType = table.coreOptions().primaryKeyVectorIndexType(vectorField.name());
        this.indexOptions = table.coreOptions().primaryKeyVectorIndexOptions(vectorField.name());
        this.searchOptions = Collections.unmodifiableMap(new HashMap<>(searchOptions));
        this.query = query.clone();
        this.limit = limit;
        this.metric =
                normalize(table.coreOptions().primaryKeyVectorDistanceMetric(vectorField.name()));
    }

    private static KeyValueFileStore keyValueStore(FileStoreTable table) {
        FileStoreTable unwrapped = table;
        while (unwrapped instanceof DelegatedFileStoreTable) {
            unwrapped = ((DelegatedFileStoreTable) unwrapped).wrapped();
        }
        checkArgument(
                unwrapped.store() instanceof KeyValueFileStore,
                "Primary-key vector search requires a key-value file store.");
        return (KeyValueFileStore) unwrapped.store();
    }

    @Override
    public GlobalIndexResult read(VectorScan.Plan plan) {
        PrimaryKeyVectorScan.Plan primaryKeyPlan = primaryKeyPlan(plan);
        return createResult(primaryKeyPlan, searchBuckets(bucketSplits(primaryKeyPlan)));
    }

    protected PrimaryKeyVectorScan.Plan primaryKeyPlan(VectorScan.Plan plan) {
        checkArgument(
                plan instanceof PrimaryKeyVectorScan.Plan,
                "Primary-key vector read requires a PrimaryKeyVectorScan plan.");
        PrimaryKeyVectorScan.Plan primaryKeyPlan = (PrimaryKeyVectorScan.Plan) plan;
        for (VectorSearchSplit searchSplit : primaryKeyPlan.splits()) {
            BucketVectorSearchSplit split = (BucketVectorSearchSplit) searchSplit;
            checkArgument(
                    split.dataSplit().snapshotId() == primaryKeyPlan.snapshotId(),
                    "Vector bucket split snapshot does not match its plan.");
        }
        return primaryKeyPlan;
    }

    protected List<BucketVectorSearchSplit> bucketSplits(PrimaryKeyVectorScan.Plan plan) {
        List<BucketVectorSearchSplit> splits = new ArrayList<>(plan.splits().size());
        for (VectorSearchSplit split : plan.splits()) {
            splits.add((BucketVectorSearchSplit) split);
        }
        return splits;
    }

    protected List<Candidate> searchBuckets(List<BucketVectorSearchSplit> splits) {
        try {
            SearchContext context = new SearchContext(table);
            List<Candidate> candidates = new ArrayList<>();
            for (BucketVectorSearchSplit split : splits) {
                candidates.addAll(search(split, context));
            }
            return topK(candidates, limit);
        } catch (IOException e) {
            throw new RuntimeException("Failed to search primary-key vector index.", e);
        }
    }

    protected GlobalIndexResult createResult(
            PrimaryKeyVectorScan.Plan plan, List<Candidate> candidates) {
        return new PrimaryKeyVectorResult(plan, topK(candidates, limit), metric);
    }

    private List<Candidate> search(BucketVectorSearchSplit split, SearchContext context)
            throws IOException {
        DataSplit dataSplit = split.dataSplit();
        List<DataFileMeta> activeFiles =
                dataSplit.dataFiles().stream()
                        .filter(PkVectorSourcePolicy::shouldRead)
                        .collect(Collectors.toList());
        PkVectorBucketIndexState state =
                PkVectorBucketIndexState.fromActivePayloads(
                        vectorField.id(), indexType, split.payloadFiles());
        Map<String, DeletionVector> deletionVectors = deletionVectors(dataSplit, context.fileIO);
        PkVectorDataFileReader.Factory readerFactory =
                new PkVectorDataFileReader.Factory(
                        context.readerFactoryBuilder,
                        dataSplit.partition(),
                        dataSplit.bucket(),
                        vectorField,
                        ((VectorType) vectorField.type()).getLength());
        PkVectorAnnSegmentSearcher annSearcher =
                new PkVectorAnnSegmentSearcher(
                        context.fileIO,
                        context.indexFileHandler.pkVectorAnnSegment(
                                dataSplit.partition(), dataSplit.bucket()),
                        vectorField,
                        indexOptions,
                        metric,
                        context.executor);
        PrimaryKeyVectorBucketSearch bucketSearch =
                new PrimaryKeyVectorBucketSearch(
                        readerFactory,
                        annSearcher,
                        searchOptions,
                        metric,
                        table.coreOptions().globalIndexSearchMode());
        List<Candidate> candidates = new ArrayList<>();
        for (PkVectorSearchResult result :
                bucketSearch.search(state, activeFiles, deletionVectors, query, limit)) {
            candidates.add(
                    new Candidate(
                            dataSplit.partition(),
                            dataSplit.bucket(),
                            result.dataFileName(),
                            result.rowPosition(),
                            result.distance()));
        }
        return candidates;
    }

    private Map<String, DeletionVector> deletionVectors(DataSplit split, FileIO fileIO)
            throws IOException {
        DeletionVector.Factory factory =
                DeletionVector.factory(
                        fileIO, split.dataFiles(), split.deletionFiles().orElse(null));
        Map<String, DeletionVector> result = new HashMap<>();
        for (DataFileMeta file : split.dataFiles()) {
            Optional<DeletionVector> deletionVector = factory.create(file.fileName());
            if (deletionVector.isPresent()) {
                result.put(file.fileName(), deletionVector.get());
            }
        }
        return result;
    }

    protected static List<Candidate> topK(List<Candidate> candidates, int limit) {
        checkArgument(limit > 0, "Vector search limit must be positive: %s.", limit);
        PriorityQueue<Candidate> nearest = new PriorityQueue<>(limit, BEST_FIRST.reversed());
        for (Candidate candidate : candidates) {
            if (nearest.size() < limit) {
                nearest.add(candidate);
            } else if (BEST_FIRST.compare(candidate, nearest.peek()) < 0) {
                nearest.poll();
                nearest.add(candidate);
            }
        }
        List<Candidate> result = new ArrayList<>(nearest);
        Collections.sort(result, BEST_FIRST);
        return Collections.unmodifiableList(result);
    }

    private static int compareBytes(byte[] left, byte[] right) {
        int count = Math.min(left.length, right.length);
        for (int i = 0; i < count; i++) {
            int comparison = Integer.compare(left[i] & 0xFF, right[i] & 0xFF);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    /** Snapshot-scoped physical row candidate. */
    public static class Candidate implements Serializable {

        private static final long serialVersionUID = 1L;

        private final BinaryRow partition;
        private final int bucket;
        private final String dataFileName;
        private final long rowPosition;
        private final float distance;

        Candidate(
                BinaryRow partition,
                int bucket,
                String dataFileName,
                long rowPosition,
                float distance) {
            this.partition = partition.copy();
            this.bucket = bucket;
            this.dataFileName = dataFileName;
            this.rowPosition = rowPosition;
            this.distance = distance;
        }

        public BinaryRow partition() {
            return partition;
        }

        public int bucket() {
            return bucket;
        }

        public String dataFileName() {
            return dataFileName;
        }

        public long rowPosition() {
            return rowPosition;
        }

        public float distance() {
            return distance;
        }
    }

    private static class SearchContext {

        private final FileIO fileIO;
        private final IndexFileHandler indexFileHandler;
        private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
        private final ExecutorService executor;

        private SearchContext(FileStoreTable table) {
            this.fileIO = table.fileIO();
            this.indexFileHandler = table.store().newIndexFileHandler();
            this.readerFactoryBuilder = keyValueStore(table).newReaderFactoryBuilder();
            this.executor =
                    GlobalIndexReadThreadPool.getExecutorService(
                            table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM));
        }
    }
}
