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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.BucketedAppendCompactManager;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.NoopCompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/** {@link AppendOnlyFileStoreWrite} for {@link org.apache.paimon.table.BucketMode#HASH_FIXED}. */
public class AppendOnlyFixedBucketFileStoreWrite extends AppendOnlyFileStoreWrite {

    private final String commitUser;

    public AppendOnlyFixedBucketFileStoreWrite(
            FileIO fileIO,
            RawFileSplitRead read,
            long schemaId,
            String commitUser,
            RowType rowType,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options,
            @Nullable DeletionVectorsMaintainer.Factory dvMaintainerFactory,
            String tableName) {
        super(
                fileIO,
                read,
                schemaId,
                rowType,
                pathFactory,
                snapshotManager,
                scan,
                options,
                dvMaintainerFactory,
                tableName);
        this.commitUser = commitUser;
    }

    @Override
    protected CompactManager getCompactManager(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoredFiles,
            ExecutorService compactExecutor,
            @Nullable DeletionVectorsMaintainer dvMaintainer) {
        if (options.writeOnly()) {
            return new NoopCompactManager();
        } else {
            Function<String, DeletionVector> dvFactory =
                    dvMaintainer != null
                            ? f -> dvMaintainer.deletionVectorOf(f).orElse(null)
                            : null;
            return new BucketedAppendCompactManager(
                    compactExecutor,
                    restoredFiles,
                    dvMaintainer,
                    options.compactionMinFileNum(),
                    options.compactionMaxFileNum().orElse(5),
                    options.targetFileSize(false),
                    files -> compactRewrite(partition, bucket, dvFactory, files),
                    compactionMetrics == null
                            ? null
                            : compactionMetrics.createReporter(partition, bucket));
        }
    }

    @Override
    protected Function<WriterContainer<InternalRow>, Boolean> createWriterCleanChecker() {
        return createConflictAwareWriterCleanChecker(commitUser, snapshotManager);
    }
}
