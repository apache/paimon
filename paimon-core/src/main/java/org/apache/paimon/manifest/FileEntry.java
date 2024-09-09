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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;

/** Entry representing a file. */
public interface FileEntry {

    FileKind kind();

    BinaryRow partition();

    int bucket();

    int level();

    String fileName();

    Identifier identifier();

    BinaryRow minKey();

    BinaryRow maxKey();

    /**
     * The same {@link Identifier} indicates that the {@link ManifestEntry} refers to the same data
     * file.
     */
    class Identifier {
        public final BinaryRow partition;
        public final int bucket;
        public final int level;
        public final String fileName;
        public final List<String> extraFiles;

        /* Cache the hash code for the string */
        private Integer hash;

        public Identifier(
                BinaryRow partition,
                int bucket,
                int level,
                String fileName,
                List<String> extraFiles) {
            this.partition = partition;
            this.bucket = bucket;
            this.level = level;
            this.fileName = fileName;
            this.extraFiles = extraFiles;
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
                    && Objects.equals(fileName, that.fileName)
                    && Objects.equals(extraFiles, that.extraFiles);
        }

        @Override
        public int hashCode() {
            if (hash == null) {
                hash = Objects.hash(partition, bucket, level, fileName, extraFiles);
            }
            return hash;
        }

        @Override
        public String toString() {
            return String.format(
                    "{%s, %d, %d, %s, %s}", partition, bucket, level, fileName, extraFiles);
        }

        public String toString(FileStorePathFactory pathFactory) {
            return pathFactory.getPartitionString(partition)
                    + ", bucket "
                    + bucket
                    + ", level "
                    + level
                    + ", file "
                    + fileName
                    + ", extraFiles "
                    + extraFiles;
        }
    }

    static <T extends FileEntry> Collection<T> mergeEntries(Iterable<T> entries) {
        LinkedHashMap<Identifier, T> map = new LinkedHashMap<>();
        mergeEntries(entries, map);
        return map.values();
    }

    static void mergeEntries(
            ManifestFile manifestFile,
            List<ManifestFileMeta> manifestFiles,
            Map<Identifier, ManifestEntry> map,
            @Nullable Integer manifestReadParallelism) {
        mergeEntries(
                readManifestEntries(manifestFile, manifestFiles, manifestReadParallelism), map);
    }

    static <T extends FileEntry> void mergeEntries(Iterable<T> entries, Map<Identifier, T> map) {
        for (T entry : entries) {
            Identifier identifier = entry.identifier();
            switch (entry.kind()) {
                case ADD:
                    Preconditions.checkState(
                            !map.containsKey(identifier),
                            "Trying to add file %s which is already added.",
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

    static Iterable<ManifestEntry> readManifestEntries(
            ManifestFile manifestFile,
            List<ManifestFileMeta> manifestFiles,
            @Nullable Integer manifestReadParallelism) {
        return sequentialBatchedExecute(
                file -> manifestFile.read(file.fileName(), file.fileSize()),
                manifestFiles,
                manifestReadParallelism);
    }

    static Set<Identifier> readDeletedEntries(
            ManifestFile manifestFile,
            List<ManifestFileMeta> manifestFiles,
            @Nullable Integer manifestReadParallelism) {
        manifestFiles =
                manifestFiles.stream()
                        .filter(file -> file.numDeletedFiles() > 0)
                        .collect(Collectors.toList());
        Function<ManifestFileMeta, List<Identifier>> processor =
                file ->
                        manifestFile
                                .read(
                                        file.fileName(),
                                        file.fileSize(),
                                        Filter.alwaysTrue(),
                                        deletedFilter())
                                .stream()
                                .map(ManifestEntry::identifier)
                                .collect(Collectors.toList());
        Iterable<Identifier> identifiers =
                sequentialBatchedExecute(processor, manifestFiles, manifestReadParallelism);
        Set<Identifier> result = new HashSet<>();
        for (Identifier identifier : identifiers) {
            result.add(identifier);
        }
        return result;
    }

    static Filter<InternalRow> deletedFilter() {
        Function<InternalRow, FileKind> getter = ManifestEntrySerializer.kindGetter();
        return row -> getter.apply(row) == FileKind.DELETE;
    }
}
