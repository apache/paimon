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

package org.apache.paimon.flink.source;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;

import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;

import java.util.List;
import java.util.UUID;

import static org.apache.paimon.io.DataFileTestUtils.row;

/** Base class for split enumerators. */
public abstract class FileSplitEnumeratorTestBase {

    protected TestingSplitEnumeratorContext<FileStoreSourceSplit> getSplitEnumeratorContext(
            int parallelism) {
        return getSplitEnumeratorContext(parallelism, parallelism);
    }

    protected TestingSplitEnumeratorContext<FileStoreSourceSplit> getSplitEnumeratorContext(
            int parallelism, int registeredReaders) {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(parallelism);
        for (int i = 0; i < registeredReaders; i++) {
            context.registerReader(i, "test-host");
        }
        return context;
    }

    protected FileStoreSourceSplit createSnapshotSplit(
            int snapshotId, int bucket, List<DataFileMeta> files) {
        return createSnapshotSplit(snapshotId, bucket, files, 1);
    }

    protected FileStoreSourceSplit createSnapshotSplit(
            int snapshotId, int bucket, List<DataFileMeta> files, int... partitions) {
        return new FileStoreSourceSplit(
                UUID.randomUUID().toString(),
                DataSplit.builder()
                        .withSnapshot(snapshotId)
                        .withPartition(row(partitions))
                        .withBucket(bucket)
                        .withDataFiles(files)
                        .isStreaming(true)
                        .withBucketPath("/temp/xxx") // not used
                        .build(),
                0);
    }
}
