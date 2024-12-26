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

package org.apache.paimon;

import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.HashIndexMaintainer;
import org.apache.paimon.index.IndexMaintainer;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.operation.BucketSelectConverter;
import org.apache.paimon.operation.KeyValueFileStoreScan;
import org.apache.paimon.operation.KeyValueFileStoreWrite;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.UserDefinedSeqComparator;
import org.apache.paimon.utils.ValueEqualiserSupplier;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.pickTransformFieldMapping;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** {@link FileStore} for querying and updating {@link KeyValue}s. */
public class KeyValueFileStore extends AbstractFileStore<KeyValue> {

    private final boolean crossPartitionUpdate;
    private final RowType bucketKeyType;
    private final RowType keyType;
    private final RowType valueType;
    private final KeyValueFieldsExtractor keyValueFieldsExtractor;
    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;
    private final Supplier<RecordEqualiser> logDedupEqualSupplier;
    private final MergeFunctionFactory<KeyValue> mfFactory;

    public KeyValueFileStore(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            boolean crossPartitionUpdate,
            CoreOptions options,
            RowType partitionType,
            RowType bucketKeyType,
            RowType keyType,
            RowType valueType,
            KeyValueFieldsExtractor keyValueFieldsExtractor,
            MergeFunctionFactory<KeyValue> mfFactory,
            String tableName,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, schemaManager, schema, tableName, options, partitionType, catalogEnvironment);
        this.crossPartitionUpdate = crossPartitionUpdate;
        this.bucketKeyType = bucketKeyType;
        this.keyType = keyType;
        this.valueType = valueType;
        this.keyValueFieldsExtractor = keyValueFieldsExtractor;
        this.mfFactory = mfFactory;
        this.keyComparatorSupplier = new KeyComparatorSupplier(keyType);
        List<String> logDedupIgnoreFields = options.changelogRowDeduplicateIgnoreFields();
        this.logDedupEqualSupplier =
                options.changelogRowDeduplicate()
                        ? ValueEqualiserSupplier.fromIgnoreFields(valueType, logDedupIgnoreFields)
                        : () -> null;
    }

    @Override
    public BucketMode bucketMode() {
        if (options.bucket() == -1) {
            return crossPartitionUpdate ? BucketMode.CROSS_PARTITION : BucketMode.HASH_DYNAMIC;
        } else {
            checkArgument(!crossPartitionUpdate);
            return BucketMode.HASH_FIXED;
        }
    }

    @Override
    public KeyValueFileStoreScan newScan() {
        return newScan(false);
    }

    @Override
    public MergeFileSplitRead newRead() {
        return new MergeFileSplitRead(
                options,
                schema,
                keyType,
                valueType,
                newKeyComparator(),
                mfFactory,
                newReaderFactoryBuilder());
    }

    public RawFileSplitRead newBatchRawFileRead() {
        return new RawFileSplitRead(
                fileIO,
                schemaManager,
                schema,
                valueType,
                FileFormatDiscover.of(options),
                pathFactory(),
                options.fileIndexReadEnabled());
    }

    public KeyValueFileReaderFactory.Builder newReaderFactoryBuilder() {
        return KeyValueFileReaderFactory.builder(
                fileIO,
                schemaManager,
                schema,
                keyType,
                valueType,
                FileFormatDiscover.of(options),
                pathFactory(),
                keyValueFieldsExtractor,
                options);
    }

    @Override
    public KeyValueFileStoreWrite newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    @Override
    public KeyValueFileStoreWrite newWrite(String commitUser, ManifestCacheFilter manifestFilter) {
        IndexMaintainer.Factory<KeyValue> indexFactory = null;
        if (bucketMode() == BucketMode.HASH_DYNAMIC) {
            indexFactory = new HashIndexMaintainer.Factory(newIndexFileHandler());
        }
        DeletionVectorsMaintainer.Factory deletionVectorsMaintainerFactory = null;
        if (options.deletionVectorsEnabled()) {
            deletionVectorsMaintainerFactory =
                    new DeletionVectorsMaintainer.Factory(newIndexFileHandler());
        }
        return new KeyValueFileStoreWrite(
                fileIO,
                schemaManager,
                schema,
                commitUser,
                partitionType,
                keyType,
                valueType,
                keyComparatorSupplier,
                () -> UserDefinedSeqComparator.create(valueType, options),
                logDedupEqualSupplier,
                mfFactory,
                pathFactory(),
                format2PathFactory(),
                snapshotManager(),
                newScan(true).withManifestCacheFilter(manifestFilter),
                indexFactory,
                deletionVectorsMaintainerFactory,
                options,
                keyValueFieldsExtractor,
                tableName);
    }

    private Map<String, FileStorePathFactory> format2PathFactory() {
        Map<String, FileStorePathFactory> pathFactoryMap = new HashMap<>();
        Set<String> formats = new HashSet<>(options.fileFormatPerLevel().values());
        formats.add(options.fileFormat().getFormatIdentifier());
        formats.forEach(format -> pathFactoryMap.put(format, pathFactory(format)));
        return pathFactoryMap;
    }

    private KeyValueFileStoreScan newScan(boolean forWrite) {
        BucketSelectConverter bucketSelectConverter =
                keyFilter -> {
                    if (bucketMode() != BucketMode.HASH_FIXED) {
                        return Optional.empty();
                    }

                    List<Predicate> bucketFilters =
                            pickTransformFieldMapping(
                                    splitAnd(keyFilter),
                                    keyType.getFieldNames(),
                                    bucketKeyType.getFieldNames());
                    if (bucketFilters.size() > 0) {
                        return BucketSelectConverter.create(and(bucketFilters), bucketKeyType);
                    }
                    return Optional.empty();
                };
        return new KeyValueFileStoreScan(
                newManifestsReader(forWrite),
                bucketSelectConverter,
                snapshotManager(),
                schemaManager,
                schema,
                keyValueFieldsExtractor,
                manifestFileFactory(forWrite),
                options.scanManifestParallelism(),
                options.deletionVectorsEnabled(),
                options.mergeEngine(),
                options.changelogProducer(),
                options.fileIndexReadEnabled() && options.deletionVectorsEnabled());
    }

    @Override
    public Comparator<InternalRow> newKeyComparator() {
        return keyComparatorSupplier.get();
    }
}
