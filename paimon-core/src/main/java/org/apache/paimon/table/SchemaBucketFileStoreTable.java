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

package org.apache.paimon.table;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.table.sink.FixedBucketWriteSelector;
import org.apache.paimon.table.sink.PartitionBucketMapping;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.sink.WriteSelector;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

/**
 * A {@link FileStoreTable} wrapper that uses the schema number of buckets assign writes instead of
 * using the number of buckets defined in each partition. Useful for postpone buckets, overrides and
 * rescales.
 */
public class SchemaBucketFileStoreTable extends DelegatedFileStoreTable {

    public SchemaBucketFileStoreTable(FileStoreTable wrapped) {
        super(wrapped);
    }

    @Override
    public Optional<WriteSelector> newWriteSelector() {
        return Optional.of(
                new FixedBucketWriteSelector(
                        schema(), new PartitionBucketMapping(schema().numBuckets())));
    }

    @Override
    public RowKeyExtractor createRowKeyExtractor() {
        return new FixedBucketRowKeyExtractor(
                schema(), new PartitionBucketMapping(schema().numBuckets()));
    }

    @Override
    public TableWriteImpl<?> newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    @Override
    public TableWriteImpl<?> newWrite(String commitUser, @Nullable Integer writeId) {
        return wrapped().newWrite(commitUser, writeId, createRowKeyExtractor());
    }

    @Override
    public TableWriteImpl<?> newWrite(
            String commitUser, @Nullable Integer writeId, RowKeyExtractor rowKeyExtractor) {
        // Always use the schema-bucket-based extractor; ignore the caller-supplied extractor
        // to ensure consistent per-partition bucket routing even when called via the 3-arg form.
        return wrapped().newWrite(commitUser, writeId, createRowKeyExtractor());
    }

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        return new SchemaBucketFileStoreTable(wrapped().copy(dynamicOptions));
    }

    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        return new SchemaBucketFileStoreTable(wrapped().copy(newTableSchema));
    }

    @Override
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new SchemaBucketFileStoreTable(wrapped().copyWithoutTimeTravel(dynamicOptions));
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        return new SchemaBucketFileStoreTable(wrapped().copyWithLatestSchema());
    }

    @Override
    public FileStoreTable switchToBranch(String branchName) {
        return new SchemaBucketFileStoreTable(wrapped().switchToBranch(branchName));
    }
}
