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
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Preconditions;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Abstract a simplest model of manifest file. */
public abstract class AbstractManifestEntry {
    protected final FileKind kind;
    protected final String fileName;
    // for tables without partition this field should be a row with 0 columns (not null)
    protected final BinaryRow partition;
    protected final int bucket;
    protected final int totalBuckets;
    protected final int level;

    public AbstractManifestEntry(
            FileKind kind,
            String fileName,
            BinaryRow partition,
            int bucket,
            int totalBuckets,
            int level) {
        this.kind = kind;
        this.fileName = fileName;
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.level = level;
    }

    public FileKind kind() {
        return kind;
    }

    public BinaryRow partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public int level() {
        return level;
    }

    public Identifier identifier() {
        return new Identifier(partition, bucket, level, fileName);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AbstractManifestEntry)) {
            return false;
        }
        AbstractManifestEntry that = (AbstractManifestEntry) o;
        return Objects.equals(kind, that.kind)
                && Objects.equals(partition, that.partition)
                && bucket == that.bucket
                && level == that.level
                && Objects.equals(fileName, that.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, partition, bucket, level, fileName);
    }

    @Override
    public String toString() {
        return String.format("{%s, %s, %d, %d, %s}", kind, partition, bucket, level, fileName);
    }

    public static <T extends AbstractManifestEntry> Collection<T> mergeEntries(
            Iterable<T> entries) {
        LinkedHashMap<Identifier, T> map = new LinkedHashMap<>();
        mergeEntries(entries, map);
        return map.values();
    }

    public static <T extends AbstractManifestEntry> void mergeEntries(
            Iterable<T> entries, Map<Identifier, T> map) {
        for (T entry : entries) {
            Identifier identifier = entry.identifier();
            switch (entry.kind()) {
                case ADD:
                    Preconditions.checkState(
                            !map.containsKey(identifier),
                            "Trying to add file %s which is already added. Manifest might be corrupted.",
                            identifier);
                    map.put(identifier, entry);
                    break;
                case DELETE:
                    // each dataFile will only be added once and deleted once,
                    // if we know that it is added before then both add and delete entry can be
                    // removed because there won't be further operations on this file,
                    // otherwise we have to keep the delete entry because the add entry must be
                    // in the previous manifest files
                    if (map.containsKey(identifier)) {
                        map.remove(identifier);
                    } else {
                        map.put(identifier, entry);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown value kind " + entry.kind().name());
            }
        }
    }

    /**
     * The same {@link Identifier} indicates that the {@link AbstractManifestEntry} refers to the
     * same data file.
     */
    public static class Identifier {
        public final BinaryRow partition;
        public final int bucket;
        public final int level;
        public final String fileName;

        private Identifier(BinaryRow partition, int bucket, int level, String fileName) {
            this.partition = partition;
            this.bucket = bucket;
            this.level = level;
            this.fileName = fileName;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Identifier)) {
                return false;
            }
            Identifier that = (Identifier) o;
            return Objects.equals(partition, that.partition)
                    && bucket == that.bucket
                    && level == that.level
                    && Objects.equals(fileName, that.fileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket, level, fileName);
        }

        @Override
        public String toString() {
            return String.format("{%s, %d, %d, %s}", partition, bucket, level, fileName);
        }

        public String toString(FileStorePathFactory pathFactory) {
            return pathFactory.getPartitionString(partition)
                    + ", bucket "
                    + bucket
                    + ", level "
                    + level
                    + ", file "
                    + fileName;
        }
    }
}
