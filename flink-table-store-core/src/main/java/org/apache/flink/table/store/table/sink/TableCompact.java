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

package org.apache.flink.table.store.table.sink;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.compact.CompactResult;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.operation.FileStoreWrite;
import org.apache.flink.table.store.file.predicate.PredicateConverter;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;

/** An abstraction layer above {@link FileStoreWrite#createCompactWriter} to provide compaction. */
public class TableCompact {

    private static final Logger LOG = LoggerFactory.getLogger(TableCompact.class);

    private final FileStoreScan scan;
    private final FileStoreWrite<?> write;
    private final RowType partitionType;

    private BiPredicate<BinaryRowData, Integer> partBucketFilter;

    public TableCompact(FileStoreScan scan, FileStoreWrite<?> write, RowType partitionType) {
        this.scan = scan;
        this.write = write;
        this.partitionType = partitionType;
    }

    public TableCompact withPartitions(Map<String, String> partitionSpec) {
        scan.withPartitionFilter(PredicateConverter.fromMap(partitionSpec, partitionType));
        return this;
    }

    public TableCompact withFilter(BiPredicate<BinaryRowData, Integer> partBucketFilter) {
        this.partBucketFilter = partBucketFilter;
        return this;
    }

    public List<FileCommittable> compact() {
        List<FileCommittable> committables = new ArrayList<>();
        scan.plan()
                .groupByPartFiles()
                .forEach(
                        (partition, buckets) ->
                                buckets.forEach(
                                        (bucket, files) ->
                                                doCompact(partition, bucket, files)
                                                        .ifPresent(committables::add)));
        return committables;
    }

    private Optional<FileCommittable> doCompact(
            BinaryRowData partition, int bucket, List<DataFileMeta> files) {
        if (!partBucketFilter.test(partition, bucket)) {
            return Optional.empty();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Do compaction for partition {}, bucket {}",
                    FileStorePathFactory.getPartitionComputer(
                                    partitionType,
                                    FileStorePathFactory.PARTITION_DEFAULT_NAME.defaultValue())
                            .generatePartValues(partition),
                    bucket);
        }
        try {
            CompactResult result =
                    write.createCompactWriter(partition.copy(), bucket, files).call();
            FileCommittable committable =
                    new FileCommittable(
                            partition,
                            bucket,
                            Increment.forCompact(result.before(), result.after()));
            return Optional.of(committable);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
