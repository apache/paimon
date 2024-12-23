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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ManifestReadThreadPool.randomlyExecuteSequentialReturn;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;

/** Entry representing a file. */
public interface FileEntry {

    FileKind kind();

    BinaryRow partition();

    int bucket();

    int level();

    String fileName();

    Identifier identifier();

    String externalPath();

    BinaryRow minKey();

    BinaryRow maxKey();

    List<String> extraFiles();

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
        @Nullable private final byte[] embeddedIndex;

        /* Cache the hash code for the string */
        private Integer hash;

        public Identifier(
                BinaryRow partition,
                int bucket,
                int level,
                String fileName,
                List<String> extraFiles,
                @Nullable byte[] embeddedIndex) {
            this.partition = partition;
            this.bucket = bucket;
            this.level = level;
            this.fileName = fileName;
            this.extraFiles = extraFiles;
            this.embeddedIndex = embeddedIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Identifier that = (Identifier) o;
            return bucket == that.bucket
                    && level == that.level
                    && Objects.equals(partition, that.partition)
                    && Objects.equals(fileName, that.fileName)
                    && Objects.equals(extraFiles, that.extraFiles)
                    && Objects.deepEquals(embeddedIndex, that.embeddedIndex);
        }

        @Override
        public int hashCode() {
            if (hash == null) {
                hash =
                        Objects.hash(
                                partition,
                                bucket,
                                level,
                                fileName,
                                extraFiles,
                                Arrays.hashCode(embeddedIndex));
            }
            return hash;
        }

        @Override
        public String toString() {
            return "{partition="
                    + partition
                    + ", bucket="
                    + bucket
                    + ", level="
                    + level
                    + ", fileName="
                    + fileName
                    + ", extraFiles="
                    + extraFiles
                    + ", embeddedIndex="
                    + Arrays.toString(embeddedIndex)
                    + '}';
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
                    + extraFiles
                    + ", embeddedIndex "
                    + Arrays.toString(embeddedIndex);
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
        return readDeletedEntries(
                m ->
                        manifestFile.read(
                                m.fileName(),
                                m.fileSize(),
                                Filter.alwaysTrue(),
                                deletedFilter(),
                                Filter.alwaysTrue()),
                manifestFiles,
                manifestReadParallelism);
    }

    static <T extends FileEntry> Set<Identifier> readDeletedEntries(
            Function<ManifestFileMeta, List<T>> manifestReader,
            List<ManifestFileMeta> manifestFiles,
            @Nullable Integer manifestReadParallelism) {
        manifestFiles =
                manifestFiles.stream()
                        .filter(file -> file.numDeletedFiles() > 0)
                        .collect(Collectors.toList());
        Function<ManifestFileMeta, List<Identifier>> processor =
                file ->
                        manifestReader.apply(file).stream()
                                // filter again, ensure is delete
                                .filter(e -> e.kind() == FileKind.DELETE)
                                .map(FileEntry::identifier)
                                .collect(Collectors.toList());
        Iterator<Identifier> identifiers =
                randomlyExecuteSequentialReturn(processor, manifestFiles, manifestReadParallelism);
        Set<Identifier> result = ConcurrentHashMap.newKeySet();
        while (identifiers.hasNext()) {
            result.add(identifiers.next());
        }
        return result;
    }

    static Filter<InternalRow> deletedFilter() {
        Function<InternalRow, FileKind> getter = ManifestEntrySerializer.kindGetter();
        return row -> getter.apply(row) == FileKind.DELETE;
    }

    static Filter<InternalRow> addFilter() {
        Function<InternalRow, FileKind> getter = ManifestEntrySerializer.kindGetter();
        return row -> getter.apply(row) == FileKind.ADD;
    }
}
