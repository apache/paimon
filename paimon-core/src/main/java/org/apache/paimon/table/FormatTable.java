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

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SimpleFileReader;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * A file format table refers to a directory that contains multiple files of the same format, where
 * operations on this table allow for reading or writing to these files, facilitating the retrieval
 * of existing data and the addition of new files.
 *
 * <p>Partitioned file format table just like the standard hive format. Partitions are discovered
 * and inferred based on directory structure.
 *
 * @since 0.9.0
 */
@Public
public interface FormatTable extends Table {

    /** Directory location in file system. */
    String location();

    /** Format of this table. */
    Format format();

    @Override
    FormatTable copy(Map<String, String> dynamicOptions);

    /** Currently supported formats. */
    enum Format {
        ORC,
        PARQUET,
        CSV
    }

    /** Create a new builder for {@link FormatTable}. */
    static FormatTable.Builder builder() {
        return new FormatTable.Builder();
    }

    /** Builder for {@link FormatTable}. */
    class Builder {

        private Identifier identifier;
        private RowType rowType;
        private List<String> partitionKeys;
        private String location;
        private FormatTable.Format format;
        private Map<String, String> options;
        @Nullable private String comment;

        public FormatTable.Builder identifier(Identifier identifier) {
            this.identifier = identifier;
            return this;
        }

        public FormatTable.Builder rowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        public FormatTable.Builder partitionKeys(List<String> partitionKeys) {
            this.partitionKeys = partitionKeys;
            return this;
        }

        public FormatTable.Builder location(String location) {
            this.location = location;
            return this;
        }

        public FormatTable.Builder format(FormatTable.Format format) {
            this.format = format;
            return this;
        }

        public FormatTable.Builder options(Map<String, String> options) {
            this.options = options;
            return this;
        }

        public FormatTable.Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        public FormatTable build() {
            return new FormatTable.FormatTableImpl(
                    identifier, rowType, partitionKeys, location, format, options, comment);
        }
    }

    /** An implementation for {@link FormatTable}. */
    class FormatTableImpl implements FormatTable {

        private final Identifier identifier;
        private final RowType rowType;
        private final List<String> partitionKeys;
        private final String location;
        private final Format format;
        private final Map<String, String> options;
        @Nullable private final String comment;

        public FormatTableImpl(
                Identifier identifier,
                RowType rowType,
                List<String> partitionKeys,
                String location,
                Format format,
                Map<String, String> options,
                @Nullable String comment) {
            this.identifier = identifier;
            this.rowType = rowType;
            this.partitionKeys = partitionKeys;
            this.location = location;
            this.format = format;
            this.options = options;
            this.comment = comment;
        }

        @Override
        public String name() {
            return identifier.getTableName();
        }

        @Override
        public String fullName() {
            return identifier.getFullName();
        }

        @Override
        public RowType rowType() {
            return rowType;
        }

        @Override
        public List<String> partitionKeys() {
            return partitionKeys;
        }

        @Override
        public List<String> primaryKeys() {
            return Collections.emptyList();
        }

        @Override
        public String location() {
            return location;
        }

        @Override
        public Format format() {
            return format;
        }

        @Override
        public Map<String, String> options() {
            return options;
        }

        @Override
        public Optional<String> comment() {
            return Optional.ofNullable(comment);
        }

        @Override
        public FormatTable copy(Map<String, String> dynamicOptions) {
            Map<String, String> newOptions = new HashMap<>(options);
            newOptions.putAll(dynamicOptions);
            return new FormatTableImpl(
                    identifier, rowType, partitionKeys, location, format, newOptions, comment);
        }
    }

    // ===================== Unsupported ===============================

    @Override
    default Optional<Statistics> statistics() {
        return Optional.empty();
    }

    @Override
    default Optional<Statistics> statistics(Long snapshotId) {
        return Optional.empty();
    }

    @Override
    default OptionalLong latestSnapshotId() {
        throw new UnsupportedOperationException();
    }

    @Override
    default Snapshot snapshot(long snapshotId) {
        throw new UnsupportedOperationException();
    }

    @Override
    default SimpleFileReader<ManifestFileMeta> manifestListReader() {
        throw new UnsupportedOperationException();
    }

    @Override
    default SimpleFileReader<ManifestEntry> manifestFileReader() {
        throw new UnsupportedOperationException();
    }

    @Override
    default SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        throw new UnsupportedOperationException();
    }

    @Override
    default void rollbackTo(long snapshotId) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void createTag(String tagName, long fromSnapshotId) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void createTag(String tagName, long fromSnapshotId, Duration timeRetained) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void createTag(String tagName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void createTag(String tagName, Duration timeRetained) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void deleteTag(String tagName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void rollbackTo(String tagName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void createBranch(String branchName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void createBranch(String branchName, String tagName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void deleteBranch(String branchName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void fastForward(String branchName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default ExpireSnapshots newExpireSnapshots() {
        throw new UnsupportedOperationException();
    }

    @Override
    default ExpireSnapshots newExpireChangelog() {
        throw new UnsupportedOperationException();
    }

    @Override
    default ReadBuilder newReadBuilder() {
        throw new UnsupportedOperationException();
    }

    @Override
    default BatchWriteBuilder newBatchWriteBuilder() {
        throw new UnsupportedOperationException();
    }

    @Override
    default StreamWriteBuilder newStreamWriteBuilder() {
        throw new UnsupportedOperationException();
    }
}
