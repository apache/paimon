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

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.SerializationUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * A lightweight, serializable sort key for {@link ManifestEntry}. Entries are ordered by {@code
 * partition -> bucket -> level -> fileName}, which co-locates the ADD and DELETE of the same file
 * (they share the same key) so they always land in the same Spark partition after {@code sortByKey}
 * and can be cancelled during the manifest rewrite. The {@code partition -> bucket -> level} prefix
 * also keeps {@link ManifestFileMeta} statistics (partitionStats / minBucket / maxBucket / minLevel
 * / maxLevel) compact for scan pruning.
 *
 * <p>The partition is kept as serialized bytes and deserialized lazily on the first comparison; the
 * generated {@link RecordComparator} is also created lazily per executor to avoid being serialized
 * across the shuffle. This class is intentionally small so that it can be used as the key of a
 * Spark {@code sortByKey} shuffle without moving the whole {@link ManifestEntry} payload.
 *
 * <p><b>Kryo compatibility:</b> the partition is stored as a plain {@code byte[]} rather than a
 * {@link BinaryRow} because this key travels through Spark's shuffle, where the serializer is
 * {@code KryoSerializer} (Paimon's Spark test base randomly picks Kryo). {@code BinaryRow} only
 * implements Java serialization (its {@code BinarySection.writeObject/readObject} callbacks), and
 * Kryo does not invoke those callbacks while also skipping the {@code transient segments} field —
 * so a {@code BinaryRow} key ends up with {@code null} segments after a Kryo shuffle and NPEs on
 * the first comparison. {@code byte[]} and {@code String} are transparent to both Kryo and Java
 * serialization, and the {@link BinaryRow} / {@link RecordComparator} are rebuilt lazily on the
 * executor after the shuffle.
 */
public class ManifestEntrySortKey implements Serializable, Comparable<ManifestEntrySortKey> {

    private static final long serialVersionUID = 1L;

    private final byte[] partitionBytes;
    private final int bucket;
    private final int level;
    private final String fileName;

    private final List<DataType> partitionFieldTypes;

    private transient BinaryRow partition;
    private transient RecordComparator partitionComparator;

    public ManifestEntrySortKey(
            BinaryRow partition,
            int bucket,
            int level,
            String fileName,
            List<DataType> partitionFieldTypes) {
        this.partitionBytes = SerializationUtils.serializeBinaryRow(partition);
        this.bucket = bucket;
        this.level = level;
        this.fileName = fileName;
        this.partitionFieldTypes = partitionFieldTypes;
    }

    @Override
    public int compareTo(ManifestEntrySortKey other) {
        // 1. partition
        int cmp = partitionComparator().compare(partition(), other.partition());
        if (cmp != 0) {
            return cmp;
        }
        // 2. bucket
        cmp = Integer.compare(bucket, other.bucket);
        if (cmp != 0) {
            return cmp;
        }
        // 3. level
        cmp = Integer.compare(level, other.level);
        if (cmp != 0) {
            return cmp;
        }
        // 4. fileName — co-locates ADD and DELETE of the same file (same key)
        return fileName.compareTo(other.fileName);
    }

    private BinaryRow partition() {
        if (partition == null) {
            partition = SerializationUtils.deserializeBinaryRow(partitionBytes);
        }
        return partition;
    }

    private RecordComparator partitionComparator() {
        if (partitionComparator == null) {
            partitionComparator = CodeGenUtils.newRecordComparator(partitionFieldTypes);
        }
        return partitionComparator;
    }

    public int bucket() {
        return bucket;
    }

    public int level() {
        return level;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ManifestEntrySortKey that = (ManifestEntrySortKey) o;
        return bucket == that.bucket
                && level == that.level
                && Objects.deepEquals(partitionBytes, that.partitionBytes)
                && Objects.equals(fileName, that.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, level, fileName, java.util.Arrays.hashCode(partitionBytes));
    }
}
