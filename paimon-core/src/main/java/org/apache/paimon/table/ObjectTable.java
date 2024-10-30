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
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SimpleFileReader;

import java.time.Duration;
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
 * @since 1.0.0
 */
@Public
public interface ObjectTable extends Table {

    RowType SCHEMA =
            RowType.builder()
                    .field("path", DataTypes.STRING().notNull())
                    .field("name", DataTypes.STRING().notNull())
                    .field("length", DataTypes.BIGINT().notNull())
                    .field("generation", DataTypes.INT())
                    .field("content_type", DataTypes.STRING())
                    .field("storage_class", DataTypes.STRING())
                    .field("mtime", DataTypes.TIMESTAMP_MILLIS())
                    .field("atime", DataTypes.TIMESTAMP_MILLIS())
                    .field("md5_hash", DataTypes.STRING())
                    .field("owner", DataTypes.STRING())
                    .field("metadata_mtime", DataTypes.TIMESTAMP_MILLIS())
                    .field("metadata", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                    .build();

    // ===================== Unsupported ===============================

    @Override
    default Optional<Statistics> statistics() {
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
    default void renameTag(String tagName, String targetTagName) {
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
