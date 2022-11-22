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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunctionFactory;
import org.apache.flink.table.store.file.operation.KeyValueFileStoreRead;
import org.apache.flink.table.store.file.operation.KeyValueFileStoreScan;
import org.apache.flink.table.store.file.operation.KeyValueFileStoreWrite;
import org.apache.flink.table.store.file.schema.KeyFieldsExtractor;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.KeyComparatorSupplier;
import org.apache.flink.table.types.logical.RowType;

import java.util.Comparator;
import java.util.function.Supplier;

/** {@link FileStore} for querying and updating {@link KeyValue}s. */
public class KeyValueFileStore extends AbstractFileStore<KeyValue> {

    private static final long serialVersionUID = 1L;

    private final RowType bucketKeyType;
    private final RowType keyType;
    private final RowType valueType;
    private final KeyFieldsExtractor keyFieldsExtractor;
    private final Supplier<Comparator<RowData>> keyComparatorSupplier;
    private final MergeFunctionFactory<KeyValue> mfFactory;

    public KeyValueFileStore(
            SchemaManager schemaManager,
            long schemaId,
            CoreOptions options,
            RowType partitionType,
            RowType bucketKeyType,
            RowType keyType,
            RowType valueType,
            KeyFieldsExtractor keyFieldsExtractor,
            MergeFunctionFactory<KeyValue> mfFactory) {
        super(schemaManager, schemaId, options, partitionType);
        this.bucketKeyType = bucketKeyType;
        this.keyType = keyType;
        this.valueType = valueType;
        this.keyFieldsExtractor = keyFieldsExtractor;
        this.mfFactory = mfFactory;
        this.keyComparatorSupplier = new KeyComparatorSupplier(keyType);
    }

    @Override
    public KeyValueFileStoreScan newScan() {
        return newScan(false);
    }

    @Override
    public KeyValueFileStoreRead newRead() {
        return new KeyValueFileStoreRead(
                schemaManager,
                schemaId,
                keyType,
                valueType,
                newKeyComparator(),
                mfFactory,
                options.fileFormat(),
                pathFactory());
    }

    @Override
    public KeyValueFileStoreWrite newWrite(String commitUser) {
        return new KeyValueFileStoreWrite(
                schemaManager,
                schemaId,
                commitUser,
                keyType,
                valueType,
                keyComparatorSupplier,
                mfFactory.create(null),
                pathFactory(),
                snapshotManager(),
                newScan(true),
                options);
    }

    private KeyValueFileStoreScan newScan(boolean checkNumOfBuckets) {
        return new KeyValueFileStoreScan(
                partitionType,
                bucketKeyType,
                keyType,
                snapshotManager(),
                schemaManager,
                schemaId,
                keyFieldsExtractor,
                manifestFileFactory(),
                manifestListFactory(),
                options.bucket(),
                checkNumOfBuckets,
                options.changelogProducer());
    }

    @Override
    public Comparator<RowData> newKeyComparator() {
        return keyComparatorSupplier.get();
    }
}
