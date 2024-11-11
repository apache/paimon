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

package org.apache.paimon.table.object;

import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.HashSet;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A object table refers to a directory that contains multiple objects (files), Object table
 * provides metadata indexes for unstructured data objects in this directory. Allowing users to
 * analyze unstructured data in Object Storage.
 *
 * <p>Object Table stores the metadata of objects in the underlying table.
 */
public interface ObjectTable extends FileStoreTable {

    RowType SCHEMA =
            RowType.builder()
                    .field("path", DataTypes.STRING().notNull())
                    .field("name", DataTypes.STRING().notNull())
                    .field("length", DataTypes.BIGINT().notNull())
                    .field("mtime", DataTypes.TIMESTAMP_LTZ_MILLIS())
                    .field("atime", DataTypes.TIMESTAMP_LTZ_MILLIS())
                    .field("owner", DataTypes.STRING().nullable())
                    .field("generation", DataTypes.INT().nullable())
                    .field("content_type", DataTypes.STRING().nullable())
                    .field("storage_class", DataTypes.STRING().nullable())
                    .field("md5_hash", DataTypes.STRING().nullable())
                    .field("metadata_mtime", DataTypes.TIMESTAMP_LTZ_MILLIS().nullable())
                    .field("metadata", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                    .build()
                    .notNull();

    /** Object location in file system. */
    String objectLocation();

    /** Underlying table to store metadata. */
    FileStoreTable underlyingTable();

    long refresh();

    @Override
    ObjectTable copy(Map<String, String> dynamicOptions);

    /** Create a new builder for {@link ObjectTable}. */
    static ObjectTable.Builder builder() {
        return new ObjectTable.Builder();
    }

    /** Builder for {@link ObjectTable}. */
    class Builder {

        private FileStoreTable underlyingTable;
        private String objectLocation;

        public ObjectTable.Builder underlyingTable(FileStoreTable underlyingTable) {
            this.underlyingTable = underlyingTable;
            checkArgument(
                    new HashSet<>(SCHEMA.getFields())
                            .containsAll(underlyingTable.rowType().getFields()),
                    "Schema of Object Table should be %s, but is %s.",
                    SCHEMA,
                    underlyingTable.rowType());
            return this;
        }

        public ObjectTable.Builder objectLocation(String objectLocation) {
            this.objectLocation = objectLocation;
            return this;
        }

        public ObjectTable build() {
            return new ObjectTableImpl(underlyingTable, objectLocation);
        }
    }

    /** An implementation for {@link ObjectTable}. */
    class ObjectTableImpl extends DelegatedFileStoreTable implements ObjectTable {

        private final String objectLocation;

        public ObjectTableImpl(FileStoreTable underlyingTable, String objectLocation) {
            super(underlyingTable);
            this.objectLocation = objectLocation;
        }

        @Override
        public BatchWriteBuilder newBatchWriteBuilder() {
            throw new UnsupportedOperationException("Object table does not support Write.");
        }

        @Override
        public StreamWriteBuilder newStreamWriteBuilder() {
            throw new UnsupportedOperationException("Object table does not support Write.");
        }

        @Override
        public TableWriteImpl<?> newWrite(String commitUser) {
            throw new UnsupportedOperationException("Object table does not support Write.");
        }

        @Override
        public TableWriteImpl<?> newWrite(String commitUser, ManifestCacheFilter manifestFilter) {
            throw new UnsupportedOperationException("Object table does not support Write.");
        }

        @Override
        public TableCommitImpl newCommit(String commitUser) {
            throw new UnsupportedOperationException("Object table does not support Commit.");
        }

        @Override
        public String objectLocation() {
            return objectLocation;
        }

        @Override
        public FileStoreTable underlyingTable() {
            return wrapped;
        }

        @Override
        public long refresh() {
            try {
                return ObjectRefresh.refresh(this);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public ObjectTable copy(Map<String, String> dynamicOptions) {
            return new ObjectTableImpl(wrapped.copy(dynamicOptions), objectLocation);
        }

        @Override
        public FileStoreTable copy(TableSchema newTableSchema) {
            return new ObjectTableImpl(wrapped.copy(newTableSchema), objectLocation);
        }

        @Override
        public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
            return new ObjectTableImpl(
                    wrapped.copyWithoutTimeTravel(dynamicOptions), objectLocation);
        }

        @Override
        public FileStoreTable copyWithLatestSchema() {
            return new ObjectTableImpl(wrapped.copyWithLatestSchema(), objectLocation);
        }

        @Override
        public FileStoreTable switchToBranch(String branchName) {
            return new ObjectTableImpl(wrapped.switchToBranch(branchName), objectLocation);
        }
    }
}
