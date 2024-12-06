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

import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** A {@link SimpleFileEntry} with {@link #fileSource}. */
public class ExpireFileEntry extends SimpleFileEntry {

    @Nullable private final FileSource fileSource;

    public ExpireFileEntry(
            FileKind kind,
            BinaryRow partition,
            int bucket,
            int level,
            String fileName,
            List<String> extraFiles,
            @Nullable byte[] embeddedIndex,
            BinaryRow minKey,
            BinaryRow maxKey,
            @Nullable FileSource fileSource) {
        super(kind, partition, bucket, level, fileName, extraFiles, embeddedIndex, minKey, maxKey);
        this.fileSource = fileSource;
    }

    public Optional<FileSource> fileSource() {
        return Optional.ofNullable(fileSource);
    }

    public static ExpireFileEntry from(ManifestEntry entry) {
        return new ExpireFileEntry(
                entry.kind(),
                entry.partition(),
                entry.bucket(),
                entry.level(),
                entry.fileName(),
                entry.file().extraFiles(),
                entry.file().embeddedIndex(),
                entry.minKey(),
                entry.maxKey(),
                entry.file().fileSource().orElse(null));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ExpireFileEntry that = (ExpireFileEntry) o;
        return fileSource == that.fileSource;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fileSource);
    }
}
