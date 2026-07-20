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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogUtils;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.format.FormatBatchWriteBuilder;
import org.apache.paimon.table.format.FormatReadBuilder;
import org.apache.paimon.table.format.FormatTablePartitionManager;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.BatchVectorSearchBuilder;
import org.apache.paimon.table.source.FullTextSearchBuilder;
import org.apache.paimon.table.source.HybridSearchBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.VectorSearchBuilder;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SimpleFileReader;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;

/**
 * A file format table refers to a directory that contains multiple files of the same format, where
 * operations on this table allow for reading or writing to these files, facilitating the retrieval
 * of existing data and the addition of new files.
 *
 * <p>A partitioned file format table uses the standard Hive directory layout. By default,
 * partitions are discovered from that layout. Format Tables with catalog-managed partitions use
 * catalog metadata for partition visibility while retaining the same physical layout.
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

    CatalogContext catalogContext();

    /**
     * The catalog partition registrations of this table, or null when its partitions are discovered
     * from the filesystem. When non-null, catalog registration decides partition visibility: a
     * partition directory that is not registered is not visible to scans.
     */
    @Experimental
    @Nullable
    default FormatTablePartitionManager partitionManager() {
        return null;
    }

    /** Currently supported formats. */
    enum Format {
        ORC,
        PARQUET,
        CSV,
        TEXT,
        JSON,
        MOSAIC
    }

    /** Parses a file format string to a corresponding {@link Format} enum constant. */
    static Format parseFormat(String fileFormat) {
        try {
            return Format.valueOf(fileFormat.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException(
                    "Format table unsupported file format: "
                            + fileFormat
                            + ". Supported formats: "
                            + Arrays.toString(Format.values()));
        }
    }

    /** Create a new builder for {@link FormatTable}. */
    static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link FormatTable}. */
    class Builder {

        private FileIO fileIO;
        private Identifier identifier;
        private RowType rowType;
        private List<String> partitionKeys;
        private String location;
        private Format format;
        private Map<String, String> options;
        @Nullable private String comment;
        private CatalogContext catalogContext;
        @Nullable private FormatTablePartitionManager partitionManager;

        public Builder fileIO(FileIO fileIO) {
            this.fileIO = fileIO;
            return this;
        }

        public Builder identifier(Identifier identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder rowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        public Builder partitionKeys(List<String> partitionKeys) {
            this.partitionKeys = partitionKeys;
            return this;
        }

        public Builder location(String location) {
            this.location = location;
            return this;
        }

        public Builder format(Format format) {
            this.format = format;
            return this;
        }

        public Builder options(Map<String, String> options) {
            this.options = options;
            return this;
        }

        public Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        public Builder catalogContext(CatalogContext catalogContext) {
            this.catalogContext = catalogContext;
            return this;
        }

        @Experimental
        public Builder partitionManager(@Nullable FormatTablePartitionManager partitionManager) {
            this.partitionManager = partitionManager;
            return this;
        }

        public FormatTable build() {
            return new FormatTableImpl(
                    fileIO,
                    identifier,
                    rowType,
                    partitionKeys,
                    location,
                    format,
                    options,
                    comment,
                    catalogContext,
                    partitionManager);
        }
    }

    /** An implementation for {@link FormatTable}. */
    class FormatTableImpl implements FormatTable {

        private static final long serialVersionUID = 1L;

        private final FileIO fileIO;
        private final Identifier identifier;
        private final RowType rowType;
        private final List<String> partitionKeys;
        private final String location;
        private final Format format;
        private final Map<String, String> options;
        @Nullable private final String comment;
        private CatalogContext catalogContext;
        @Nullable private final FormatTablePartitionManager partitionManager;

        public FormatTableImpl(
                FileIO fileIO,
                Identifier identifier,
                RowType rowType,
                List<String> partitionKeys,
                String location,
                Format format,
                Map<String, String> options,
                @Nullable String comment,
                CatalogContext catalogContext,
                @Nullable FormatTablePartitionManager partitionManager) {
            this.fileIO = fileIO;
            this.identifier = identifier;
            this.rowType = rowType;
            this.partitionKeys = partitionKeys;
            this.location = location;
            this.format = format;
            this.options = options;
            this.comment = comment;
            this.catalogContext = catalogContext;
            this.partitionManager = partitionManager;
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
        public FileIO fileIO() {
            return fileIO;
        }

        @Override
        public FormatTable copy(Map<String, String> dynamicOptions) {
            Map<String, String> newOptions = new HashMap<>(options);
            newOptions.putAll(dynamicOptions);

            CoreOptions coreOptions = CoreOptions.fromMap(options);
            CoreOptions copiedCoreOptions = CoreOptions.fromMap(newOptions);
            boolean hasCatalogManagedPartitions = partitionManager != null;
            boolean persistedPartitionsFromCatalog = coreOptions.formatTablePartitionsFromCatalog();
            boolean dynamicPartitionsFromCatalog =
                    copiedCoreOptions.formatTablePartitionsFromCatalog();
            if (persistedPartitionsFromCatalog != dynamicPartitionsFromCatalog) {
                throw new IllegalArgumentException(
                        String.format(
                                "Dynamic option '%s' cannot change where a Format Table's partitions come from.",
                                CoreOptions.FORMAT_TABLE_PARTITION_SOURCE.key()));
            }
            if (hasCatalogManagedPartitions
                    && coreOptions.formatTablePartitionOnlyValueInPath()
                            != copiedCoreOptions.formatTablePartitionOnlyValueInPath()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Dynamic option '%s' cannot change the physical partition layout of a Format Table with catalog-managed partitions.",
                                CoreOptions.FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH.key()));
            }
            if (hasCatalogManagedPartitions) {
                CatalogUtils.validateCatalogManagedPartitionOptions(newOptions);
            }

            return new FormatTableImpl(
                    fileIO,
                    identifier,
                    rowType,
                    partitionKeys,
                    location,
                    format,
                    newOptions,
                    comment,
                    catalogContext,
                    partitionManager);
        }

        @Override
        public VectorSearchBuilder newVectorSearchBuilder() {
            throw new UnsupportedOperationException("FormatTable does not support vector search.");
        }

        @Override
        public HybridSearchBuilder newHybridSearchBuilder() {
            throw new UnsupportedOperationException("FormatTable does not support hybrid search.");
        }

        @Override
        public BatchVectorSearchBuilder newBatchVectorSearchBuilder() {
            throw new UnsupportedOperationException("FormatTable does not support vector search.");
        }

        @Override
        public FullTextSearchBuilder newFullTextSearchBuilder() {
            throw new UnsupportedOperationException(
                    "FormatTable does not support full-text search.");
        }

        @Override
        public CatalogContext catalogContext() {
            return this.catalogContext;
        }

        @Override
        @Nullable
        public FormatTablePartitionManager partitionManager() {
            return partitionManager;
        }
    }

    @Override
    default ReadBuilder newReadBuilder() {
        return new FormatReadBuilder(this);
    }

    @Override
    default BatchWriteBuilder newBatchWriteBuilder() {
        return new FormatBatchWriteBuilder(this);
    }

    default RowType partitionType() {
        return rowType().project(partitionKeys());
    }

    default String defaultPartName() {
        return options()
                .getOrDefault(PARTITION_DEFAULT_NAME.key(), PARTITION_DEFAULT_NAME.defaultValue());
    }

    // ===================== Unsupported ===============================

    @Override
    default Optional<Statistics> statistics() {
        return Optional.empty();
    }

    @Override
    default Optional<Snapshot> latestSnapshot() {
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
    default void renameTag(String tagName, String targetTagName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void replaceTag(String tagName, Long fromSnapshotId, Duration timeRetained) {
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
    default void rollbackSchema(long schemaId) {
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
    default void createBranch(String branchName, boolean ignoreIfExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void createBranch(String branchName, String tagName, boolean ignoreIfExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void deleteBranch(String branchName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void renameBranch(String fromBranch, String toBranch) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void fastForward(String branchName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void mergeBranch(String sourceBranch, String targetBranch) {
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
    default StreamWriteBuilder newStreamWriteBuilder() {
        throw new UnsupportedOperationException();
    }
}
