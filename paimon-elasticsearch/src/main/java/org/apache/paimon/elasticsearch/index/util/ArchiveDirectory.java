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

package org.apache.paimon.elasticsearch.index.util;

import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Read-only Lucene {@link org.apache.lucene.store.Directory} backed by a slice map into the
 * paimon-elasticsearch archive file.
 *
 * <p>The archive packs all Lucene segment files (e.g. {@code _0.vec}, {@code _0.vemf}, {@code
 * _0.fnm}, {@code segments_1}, ...) into one OSS object. This Directory makes Lucene see those
 * files as if they lived in a regular FS, but every {@code openInput} just hands out an {@link
 * ArchiveBackedIndexInput} that reads its bytes from the archive on demand.
 */
public class ArchiveDirectory extends BaseDirectory {

    /** {@code fileName -> [absoluteOffsetInArchive, length]}. */
    private final Map<String, long[]> fileOffsets;

    private final ArchiveFlatVectorReader.ArchiveByteRangeReader archive;

    public ArchiveDirectory(
            Map<String, long[]> fileOffsets,
            ArchiveFlatVectorReader.ArchiveByteRangeReader archive) {
        super(NoLockFactory.INSTANCE);
        this.fileOffsets = fileOffsets;
        this.archive = archive;
    }

    @Override
    public String[] listAll() {
        return fileOffsets.keySet().toArray(new String[0]);
    }

    @Override
    public long fileLength(String name) throws IOException {
        long[] r = fileOffsets.get(name);
        if (r == null) {
            throw new NoSuchFileException(name);
        }
        return r[1];
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        long[] r = fileOffsets.get(name);
        if (r == null) {
            throw new NoSuchFileException(name);
        }
        return new ArchiveBackedIndexInput("archive(" + name + ")", archive, r[0], r[1], context);
    }

    // -------- read-only: write methods are unsupported --------

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        throw new UnsupportedOperationException("ArchiveDirectory is read-only");
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw new UnsupportedOperationException("ArchiveDirectory is read-only");
    }

    @Override
    public void deleteFile(String name) {
        throw new UnsupportedOperationException("ArchiveDirectory is read-only");
    }

    @Override
    public void rename(String source, String dest) {
        throw new UnsupportedOperationException("ArchiveDirectory is read-only");
    }

    @Override
    public void sync(Collection<String> names) {
        // no-op for read-only
    }

    @Override
    public void syncMetaData() {
        // no-op for read-only
    }

    @Override
    public Set<String> getPendingDeletions() {
        return Collections.emptySet();
    }

    @Override
    public void close() {
        // Underlying archive stream is owned by the caller.
    }
}
