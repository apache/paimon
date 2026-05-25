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

package org.apache.paimon.flink.postpone;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.postpone.BucketFiles;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rewrite committable from postpone bucket table compactor. It moves all new files into compact
 * results, and delete unused new files, because compactor only produce compact snapshots.
 */
public class PostponeBucketCommittableRewriter {

    private final FileStoreTable table;
    private final FileStorePathFactory pathFactory;
    private final Map<BinaryRow, Map<Integer, BucketFiles>> buckets;

    public PostponeBucketCommittableRewriter(FileStoreTable table) {
        this.table = table;
        this.pathFactory = table.store().pathFactory();
        this.buckets = new HashMap<>();
    }

    public void add(CommitMessageImpl message) {
        buckets.computeIfAbsent(message.partition(), p -> new HashMap<>())
                .computeIfAbsent(
                        message.bucket(),
                        b ->
                                new BucketFiles(
                                        pathFactory.createDataFilePathFactory(
                                                message.partition(), message.bucket()),
                                        table.fileIO()))
                .update(message);
    }

    public List<Committable> emitAll(long checkpointId) {
        List<Committable> result = new ArrayList<>();
        for (Map.Entry<BinaryRow, Map<Integer, BucketFiles>> partitionEntry : buckets.entrySet()) {
            for (Map.Entry<Integer, BucketFiles> bucketEntry :
                    partitionEntry.getValue().entrySet()) {
                BucketFiles bucketFiles = bucketEntry.getValue();
                Committable committable =
                        new Committable(
                                checkpointId,
                                bucketFiles.makeMessage(
                                        partitionEntry.getKey(), bucketEntry.getKey()));
                result.add(committable);
            }
        }
        buckets.clear();
        return result;
    }
}
