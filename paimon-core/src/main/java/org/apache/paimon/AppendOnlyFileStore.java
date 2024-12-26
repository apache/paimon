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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.operation.AppendOnlyFileStoreScan;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.operation.AppendOnlyFixedBucketFileStoreWrite;
import org.apache.paimon.operation.AppendOnlyUnawareBucketFileStoreWrite;
import org.apache.paimon.operation.BucketSelectConverter;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.types.RowType;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.pickTransformFieldMapping;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/** {@link FileStore} for reading and writing {@link InternalRow}. */
public class AppendOnlyFileStore extends AbstractFileStore<InternalRow> {

    private final RowType bucketKeyType;
    private final RowType rowType;

    public AppendOnlyFileStore(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            CoreOptions options,
            RowType partitionType,
            RowType bucketKeyType,
            RowType rowType,
            String tableName,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, schemaManager, schema, tableName, options, partitionType, catalogEnvironment);
        this.bucketKeyType = bucketKeyType;
        this.rowType = rowType;
    }

    @Override
    public BucketMode bucketMode() {
        return options.bucket() == -1 ? BucketMode.BUCKET_UNAWARE : BucketMode.HASH_FIXED;
    }

    @Override
    public AppendOnlyFileStoreScan newScan() {
        return newScan(false);
    }

    @Override
    public RawFileSplitRead newRead() {
        return new RawFileSplitRead(
                fileIO,
                schemaManager,
                schema,
                rowType,
                FileFormatDiscover.of(options),
                pathFactory(),
                options.fileIndexReadEnabled());
    }

    @Override
    public AppendOnlyFileStoreWrite newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    @Override
    public AppendOnlyFileStoreWrite newWrite(
            String commitUser, ManifestCacheFilter manifestFilter) {
        DeletionVectorsMaintainer.Factory dvMaintainerFactory =
                options.deletionVectorsEnabled()
                        ? DeletionVectorsMaintainer.factory(newIndexFileHandler())
                        : null;
        if (bucketMode() == BucketMode.BUCKET_UNAWARE) {
            return new AppendOnlyUnawareBucketFileStoreWrite(
                    fileIO,
                    newRead(),
                    schema.id(),
                    rowType,
                    partitionType,
                    options.fileFormat(),
                    pathFactory(),
                    snapshotManager(),
                    newScan(true).withManifestCacheFilter(manifestFilter),
                    options,
                    dvMaintainerFactory,
                    tableName);
        } else {
            return new AppendOnlyFixedBucketFileStoreWrite(
                    fileIO,
                    newRead(),
                    schema.id(),
                    commitUser,
                    rowType,
                    partitionType,
                    options.fileFormat(),
                    pathFactory(),
                    snapshotManager(),
                    newScan(true).withManifestCacheFilter(manifestFilter),
                    options,
                    dvMaintainerFactory,
                    tableName);
        }
    }

    private AppendOnlyFileStoreScan newScan(boolean forWrite) {
        BucketSelectConverter bucketSelectConverter =
                predicate -> {
                    if (bucketMode() != BucketMode.HASH_FIXED) {
                        return Optional.empty();
                    }

                    if (bucketKeyType.getFieldCount() == 0) {
                        return Optional.empty();
                    }

                    List<Predicate> bucketFilters =
                            pickTransformFieldMapping(
                                    splitAnd(predicate),
                                    rowType.getFieldNames(),
                                    bucketKeyType.getFieldNames());
                    if (!bucketFilters.isEmpty()) {
                        return BucketSelectConverter.create(and(bucketFilters), bucketKeyType);
                    }
                    return Optional.empty();
                };

        return new AppendOnlyFileStoreScan(
                newManifestsReader(forWrite),
                bucketSelectConverter,
                snapshotManager(),
                schemaManager,
                schema,
                manifestFileFactory(forWrite),
                options.scanManifestParallelism(),
                options.fileIndexReadEnabled());
    }

    @Override
    public Comparator<InternalRow> newKeyComparator() {
        return null;
    }
}
