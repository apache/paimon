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

package org.apache.flink.table.store.file;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.plan.utils.SortUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.mergetree.compact.Accumulator;
import org.apache.flink.table.store.file.operation.FileStoreCommitImpl;
import org.apache.flink.table.store.file.operation.FileStoreExpireImpl;
import org.apache.flink.table.store.file.operation.FileStoreReadImpl;
import org.apache.flink.table.store.file.operation.FileStoreScanImpl;
import org.apache.flink.table.store.file.operation.FileStoreWriteImpl;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.stream.IntStream;

/** File store implementation. */
public class FileStoreImpl implements FileStore {

    private final FileStoreOptions options;
    private final String user;
    private final RowType partitionType;
    private final RowType keyType;
    private final RowType valueType;
    private final Accumulator accumulator;
    private final GeneratedRecordComparator genRecordComparator;

    public FileStoreImpl(
            Configuration options,
            String user,
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            Accumulator accumulator) {
        this.options = new FileStoreOptions(options);
        this.user = user;
        this.partitionType = partitionType;
        this.keyType = keyType;
        this.valueType = valueType;
        this.accumulator = accumulator;
        this.genRecordComparator =
                new SortCodeGenerator(
                                new TableConfig(),
                                RowType.of(keyType.getChildren().toArray(new LogicalType[0])),
                                SortUtil.getAscendingSortSpec(
                                        IntStream.range(0, keyType.getFieldCount()).toArray()))
                        .generateRecordComparator("KeyComparator");
    }

    @VisibleForTesting
    public FileStorePathFactory pathFactory() {
        return new FileStorePathFactory(
                options.path(), partitionType, options.partitionDefaultName());
    }

    private ManifestFile.Factory manifestFileFactory() {
        return new ManifestFile.Factory(
                partitionType, keyType, valueType, options.manifestFormat(), pathFactory());
    }

    @VisibleForTesting
    public ManifestList.Factory manifestListFactory() {
        return new ManifestList.Factory(partitionType, options.manifestFormat(), pathFactory());
    }

    private RecordComparator newKeyComparator() {
        return genRecordComparator.newInstance(Thread.currentThread().getContextClassLoader());
    }

    @Override
    public FileStoreWriteImpl newWrite() {
        return new FileStoreWriteImpl(
                keyType,
                valueType,
                newKeyComparator(),
                accumulator,
                options.fileFormat(),
                pathFactory(),
                newScan(),
                options.mergeTreeOptions());
    }

    @Override
    public FileStoreReadImpl newRead() {
        return new FileStoreReadImpl(
                keyType,
                valueType,
                newKeyComparator(),
                accumulator,
                options.fileFormat(),
                pathFactory());
    }

    @Override
    public FileStoreCommitImpl newCommit() {
        return new FileStoreCommitImpl(
                user,
                partitionType,
                pathFactory(),
                manifestFileFactory(),
                manifestListFactory(),
                newScan(),
                options.bucket(),
                options.manifestTargetSize(),
                options.manifestMergeMinCount());
    }

    @Override
    public FileStoreExpireImpl newExpire() {
        return new FileStoreExpireImpl(
                options.snapshotNumRetain(),
                options.snapshotTimeRetain().toMillis(),
                pathFactory(),
                manifestListFactory(),
                newScan());
    }

    @Override
    public FileStoreScanImpl newScan() {
        return new FileStoreScanImpl(
                partitionType, pathFactory(), manifestFileFactory(), manifestListFactory());
    }
}
