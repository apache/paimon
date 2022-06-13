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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.PartialUpdateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.ValueCountMergeFunction;
import org.apache.flink.table.store.file.operation.FileStoreCommitImpl;
import org.apache.flink.table.store.file.operation.FileStoreExpireImpl;
import org.apache.flink.table.store.file.operation.FileStoreReadImpl;
import org.apache.flink.table.store.file.operation.FileStoreScanImpl;
import org.apache.flink.table.store.file.operation.FileStoreWriteImpl;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.KeyComparatorSupplier;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** File store implementation. */
public class FileStoreImpl implements FileStore {

    private final SchemaManager schemaManager;
    private final long schemaId;
    private final WriteMode writeMode;
    private final FileStoreOptions options;
    private final String user;
    private final RowType partitionType;
    private final RowType keyType;
    private final RowType valueType;
    private final Supplier<Comparator<RowData>> keyComparatorSupplier;
    @Nullable private final MergeFunction mergeFunction;

    public FileStoreImpl(
            SchemaManager schemaManager,
            long schemaId,
            FileStoreOptions options,
            WriteMode writeMode,
            String user,
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            @Nullable MergeFunction mergeFunction) {
        this.schemaManager = schemaManager;
        this.schemaId = schemaId;
        this.options = options;
        this.writeMode = writeMode;
        this.user = user;
        this.partitionType = partitionType;
        this.keyType = keyType;
        this.valueType = valueType;
        this.mergeFunction = mergeFunction;
        this.keyComparatorSupplier = new KeyComparatorSupplier(keyType);
    }

    public FileStorePathFactory pathFactory() {
        return new FileStorePathFactory(
                options.path(),
                partitionType,
                options.partitionDefaultName(),
                options.fileFormat().getFormatIdentifier());
    }

    public FileStoreOptions options() {
        return options;
    }

    @VisibleForTesting
    public ManifestFile.Factory manifestFileFactory() {
        return new ManifestFile.Factory(
                schemaManager,
                schemaId,
                partitionType,
                options.manifestFormat(),
                pathFactory(),
                options.manifestTargetSize().getBytes());
    }

    @VisibleForTesting
    public ManifestList.Factory manifestListFactory() {
        return new ManifestList.Factory(partitionType, options.manifestFormat(), pathFactory());
    }

    @Override
    public FileStoreWriteImpl newWrite() {
        return new FileStoreWriteImpl(
                writeMode,
                schemaManager,
                schemaId,
                keyType,
                valueType,
                keyComparatorSupplier,
                mergeFunction,
                options.fileFormat(),
                pathFactory(),
                snapshotManager(),
                newScan(),
                options.mergeTreeOptions());
    }

    @Override
    public FileStoreReadImpl newRead() {
        return new FileStoreReadImpl(
                schemaManager,
                schemaId,
                writeMode,
                keyType,
                valueType,
                keyComparatorSupplier.get(),
                mergeFunction,
                options.fileFormat(),
                pathFactory());
    }

    @Override
    public FileStoreCommitImpl newCommit() {
        return new FileStoreCommitImpl(
                schemaId,
                user,
                partitionType,
                pathFactory(),
                snapshotManager(),
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
                options.snapshotNumRetainMin(),
                options.snapshotNumRetainMax(),
                options.snapshotTimeRetain().toMillis(),
                pathFactory(),
                snapshotManager(),
                manifestFileFactory(),
                manifestListFactory());
    }

    @Override
    public FileStoreScanImpl newScan() {
        return new FileStoreScanImpl(
                partitionType,
                keyType,
                valueType,
                snapshotManager(),
                manifestFileFactory(),
                manifestListFactory(),
                options.bucket());
    }

    @Override
    public SnapshotManager snapshotManager() {
        return new SnapshotManager(options.path());
    }

    @Override
    public RowType keyType() {
        return keyType;
    }

    @Override
    public RowType valueType() {
        return valueType;
    }

    @Override
    public RowType partitionType() {
        return partitionType;
    }

    public static FileStoreImpl createWithAppendOnly(
            SchemaManager schemaManager,
            long schemaId,
            FileStoreOptions options,
            String user,
            RowType partitionType,
            RowType rowType) {
        return new FileStoreImpl(
                schemaManager,
                schemaId,
                options,
                WriteMode.APPEND_ONLY,
                user,
                partitionType,
                RowType.of(),
                rowType,
                null);
    }

    public static FileStoreImpl createWithPrimaryKey(
            SchemaManager schemaManager,
            long schemaId,
            FileStoreOptions options,
            String user,
            RowType partitionType,
            RowType primaryKeyType,
            RowType rowType,
            FileStoreOptions.MergeEngine mergeEngine) {
        // add _KEY_ prefix to avoid conflict with value
        RowType keyType =
                new RowType(
                        primaryKeyType.getFields().stream()
                                .map(
                                        f ->
                                                new RowType.RowField(
                                                        "_KEY_" + f.getName(),
                                                        f.getType(),
                                                        f.getDescription().orElse(null)))
                                .collect(Collectors.toList()));

        MergeFunction mergeFunction;
        switch (mergeEngine) {
            case DEDUPLICATE:
                mergeFunction = new DeduplicateMergeFunction();
                break;
            case PARTIAL_UPDATE:
                List<LogicalType> fieldTypes = rowType.getChildren();
                RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.size()];
                for (int i = 0; i < fieldTypes.size(); i++) {
                    fieldGetters[i] = RowData.createFieldGetter(fieldTypes.get(i), i);
                }
                mergeFunction = new PartialUpdateMergeFunction(fieldGetters);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported merge engine: " + mergeEngine);
        }

        return new FileStoreImpl(
                schemaManager,
                schemaId,
                options,
                WriteMode.CHANGE_LOG,
                user,
                partitionType,
                keyType,
                rowType,
                mergeFunction);
    }

    public static FileStoreImpl createWithValueCount(
            SchemaManager schemaManager,
            long schemaId,
            FileStoreOptions options,
            String user,
            RowType partitionType,
            RowType rowType) {
        RowType countType =
                RowType.of(
                        new LogicalType[] {new BigIntType(false)}, new String[] {"_VALUE_COUNT"});
        MergeFunction mergeFunction = new ValueCountMergeFunction();
        return new FileStoreImpl(
                schemaManager,
                schemaId,
                options,
                WriteMode.CHANGE_LOG,
                user,
                partitionType,
                rowType,
                countType,
                mergeFunction);
    }
}
