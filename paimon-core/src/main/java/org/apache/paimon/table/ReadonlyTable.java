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
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.WriteSelector;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.utils.SimpleFileReader;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/** Readonly table which only provide implementation for scan and read. */
public interface ReadonlyTable extends InnerTable {

    @Override
    default List<String> partitionKeys() {
        return Collections.emptyList();
    }

    @Override
    default Map<String, String> options() {
        return Collections.emptyMap();
    }

    @Override
    default Optional<String> comment() {
        return Optional.empty();
    }

    @Override
    default Optional<Statistics> statistics() {
        return Optional.empty();
    }

    @Override
    default Optional<Statistics> statistics(long snapshotId) {
        return Optional.empty();
    }

    @Override
    default BatchWriteBuilder newBatchWriteBuilder() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support newBatchWriteBuilder.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default StreamWriteBuilder newStreamWriteBuilder() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support newStreamWriteBuilder.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default Optional<WriteSelector> newWriteSelector() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support newWriteSelector.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default InnerTableWrite newWrite(String commitUser) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support newWrite.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default InnerTableCommit newCommit(String commitUser) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support newCommit.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default StreamDataTableScan newStreamScan() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support newStreamScan.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default OptionalLong latestSnapshotId() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support currentSnapshot.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default Snapshot snapshot(long snapshotId) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support snapshot.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default SimpleFileReader<ManifestFileMeta> manifestListReader() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support manifestListReader.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default SimpleFileReader<ManifestEntry> manifestFileReader() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support manifestFileReader.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support indexManifestFileReader.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void rollbackTo(long snapshotId) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support rollbackTo snapshot.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void createTag(String tagName, long fromSnapshotId) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support createTag.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void createTag(String tagName, long fromSnapshotId, Duration timeRetained) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support createTag.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void createTag(String tagName) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support createTag.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void createTag(String tagName, Duration timeRetained) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support createTag.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void deleteTag(String tagName) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support deleteTag.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void rollbackTo(String tagName) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support rollbackTo tag.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void createBranch(String branchName) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support create empty branch.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void createBranch(String branchName, String tagName) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support createBranch.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void deleteBranch(String branchName) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support deleteBranch.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void fastForward(String branchName) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support fastForward.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default ExpireSnapshots newExpireSnapshots() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support expireSnapshots.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default ExpireSnapshots newExpireChangelog() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support expireChangelog.",
                        this.getClass().getSimpleName()));
    }
}
