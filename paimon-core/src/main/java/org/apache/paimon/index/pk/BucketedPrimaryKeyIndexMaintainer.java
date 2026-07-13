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

package org.apache.paimon.index.pk;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pkvector.BucketedVectorIndexMaintainer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

/** Coordinates source-backed primary-key indexes for one bucket. */
public final class BucketedPrimaryKeyIndexMaintainer {

    private final BucketedVectorIndexMaintainer vectorMaintainer;

    private BucketedPrimaryKeyIndexMaintainer(BucketedVectorIndexMaintainer vectorMaintainer) {
        this.vectorMaintainer = vectorMaintainer;
    }

    public static BucketedPrimaryKeyIndexMaintainer ofVector(
            BucketedVectorIndexMaintainer vectorMaintainer) {
        return new BucketedPrimaryKeyIndexMaintainer(vectorMaintainer);
    }

    public void prepareCommit(
            DataIncrement appendIncrement,
            CompactIncrement compactIncrement,
            boolean waitCompaction)
            throws Exception {
        BucketedVectorIndexMaintainer.VectorIndexCommit vectorCommit =
                vectorMaintainer.prepareCommit(appendIncrement, compactIncrement, waitCompaction);
        vectorCommit
                .appendIncrement()
                .ifPresent(
                        increment ->
                                applyIndexIncrement(
                                        appendIncrement.newIndexFiles(),
                                        appendIncrement.deletedIndexFiles(),
                                        increment.newIndexFiles(),
                                        increment.deletedIndexFiles()));
        vectorCommit
                .compactIncrement()
                .ifPresent(
                        increment ->
                                applyIndexIncrement(
                                        compactIncrement.newIndexFiles(),
                                        compactIncrement.deletedIndexFiles(),
                                        increment.newIndexFiles(),
                                        increment.deletedIndexFiles()));
    }

    private static void applyIndexIncrement(
            List<IndexFileMeta> targetNew,
            List<IndexFileMeta> targetDeleted,
            List<IndexFileMeta> sourceNew,
            List<IndexFileMeta> sourceDeleted) {
        targetNew.addAll(sourceNew);
        targetDeleted.addAll(sourceDeleted);
    }

    public boolean buildNotCompleted() {
        return vectorMaintainer.buildNotCompleted();
    }

    public void withExecutor(ExecutorService executor) {
        vectorMaintainer.withExecutor(executor);
    }

    public void close() {
        vectorMaintainer.close();
    }

    /** Factory to restore configured source-backed primary-key indexes for a bucket. */
    public static final class Factory {

        private final BucketedVectorIndexMaintainer.Factory vectorFactory;

        private Factory(BucketedVectorIndexMaintainer.Factory vectorFactory) {
            this.vectorFactory = vectorFactory;
        }

        public static Factory ofVector(BucketedVectorIndexMaintainer.Factory vectorFactory) {
            return new Factory(vectorFactory);
        }

        public IndexFileHandler indexFileHandler() {
            return vectorFactory.indexFileHandler();
        }

        public BucketedPrimaryKeyIndexMaintainer create(
                BinaryRow partition,
                int bucket,
                @Nullable List<DataFileMeta> restoredDataFiles,
                @Nullable List<IndexFileMeta> restoredPayloads,
                ExecutorService executor) {
            List<DataFileMeta> dataFiles =
                    restoredDataFiles == null ? Collections.emptyList() : restoredDataFiles;
            List<IndexFileMeta> payloads =
                    restoredPayloads == null ? Collections.emptyList() : restoredPayloads;
            return BucketedPrimaryKeyIndexMaintainer.ofVector(
                    vectorFactory.create(partition, bucket, dataFiles, payloads, executor));
        }

        public BucketedPrimaryKeyIndexMaintainer create(
                BinaryRow partition,
                int bucket,
                @Nullable List<DataFileMeta> restoredDataFiles,
                @Nullable List<IndexFileMeta> restoredPayloads,
                ExecutorService executor,
                @Nullable IOManager ioManager) {
            return create(partition, bucket, restoredDataFiles, restoredPayloads, executor);
        }
    }
}
