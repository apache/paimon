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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/** A {@link ManifestEntry} using pojo objects. */
public class PojoManifestEntry implements ManifestEntry {

    private final FileKind kind;
    // for tables without partition this field should be a row with 0 columns (not null)
    private final BinaryRow partition;
    private final int bucket;
    private final int totalBuckets;
    private final DataFileMeta file;

    public PojoManifestEntry(
            FileKind kind, BinaryRow partition, int bucket, int totalBuckets, DataFileMeta file) {
        this.kind = kind;
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.file = file;
    }

    @Override
    public FileKind kind() {
        return kind;
    }

    @Override
    public BinaryRow partition() {
        return partition;
    }

    @Override
    public int bucket() {
        return bucket;
    }

    @Override
    public int level() {
        return file.level();
    }

    @Override
    public String fileName() {
        return file.fileName();
    }

    @Nullable
    @Override
    public String externalPath() {
        return file.externalPath().orElse(null);
    }

    @Override
    public BinaryRow minKey() {
        return file.minKey();
    }

    @Override
    public BinaryRow maxKey() {
        return file.maxKey();
    }

    @Override
    public List<String> extraFiles() {
        return file.extraFiles();
    }

    @Override
    public int totalBuckets() {
        return totalBuckets;
    }

    @Override
    public DataFileMeta file() {
        return file;
    }

    @Override
    public Identifier identifier() {
        return new Identifier(
                partition,
                bucket,
                file.level(),
                file.fileName(),
                file.extraFiles(),
                file.embeddedIndex(),
                externalPath());
    }

    @Override
    public PojoManifestEntry copyWithoutStats() {
        return new PojoManifestEntry(
                kind, partition, bucket, totalBuckets, file.copyWithoutStats());
    }

    @Override
    public PojoManifestEntry assignSequenceNumber(long minSequenceNumber, long maxSequenceNumber) {
        return new PojoManifestEntry(
                kind,
                partition,
                bucket,
                totalBuckets,
                file.assignSequenceNumber(minSequenceNumber, maxSequenceNumber));
    }

    @Override
    public PojoManifestEntry assignFirstRowId(long firstRowId) {
        return new PojoManifestEntry(
                kind, partition, bucket, totalBuckets, file.assignFirstRowId(firstRowId));
    }

    @Override
    public ManifestEntry upgrade(int newLevel) {
        return new PojoManifestEntry(kind, partition, bucket, totalBuckets, file.upgrade(newLevel));
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ManifestEntry)) {
            return false;
        }
        ManifestEntry that = (ManifestEntry) o;
        return Objects.equals(kind, that.kind())
                && Objects.equals(partition, that.partition())
                && bucket == that.bucket()
                && totalBuckets == that.totalBuckets()
                && Objects.equals(file, that.file());
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, partition, bucket, totalBuckets, file);
    }

    @Override
    public String toString() {
        return String.format("{%s, %s, %d, %d, %s}", kind, partition, bucket, totalBuckets, file);
    }
}
