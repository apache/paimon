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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FileUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;

/** Query-local reader of full manifests or a sequence of cache-missing physical blocks. */
final class ManifestBlockReader implements Closeable {

    private final FileIO fileIO;
    private final Path path;
    @Nullable private final ManifestSidecar.Selection selected;
    @Nullable private ManifestAvroReader reader;
    @Nullable private ManifestAvroReader.RawBlock current;
    @Nullable private BlockKey currentKey;
    private int position;
    private long firstRecord;

    ManifestBlockReader(FileIO fileIO, Path path, @Nullable ManifestSidecar.Selection selected) {
        this.fileIO = fileIO;
        this.path = path;
        this.selected = selected;
    }

    private ManifestAvroReader reader() throws IOException {
        if (reader == null) {
            try {
                reader =
                        new ManifestAvroReader(
                                ManifestSidecar.openManifest(fileIO, path, selected));
            } catch (IOException e) {
                FileUtils.checkExists(fileIO, path);
                throw e;
            }
        }
        return reader;
    }

    byte[] header() throws IOException {
        return reader().headerBytes();
    }

    boolean hasNext() throws IOException {
        return reader().hasNext();
    }

    ManifestSidecar.Block next() throws IOException {
        ManifestAvroReader input = reader();
        current = input.next();
        ManifestSidecar.Block block;
        if (selected == null) {
            block =
                    new ManifestSidecar.Block(
                            input.blockOffset(),
                            input.blockLength(),
                            firstRecord,
                            current.recordCount());
        } else {
            if (position >= selected.blocks().size()) {
                throw new IOException("Unexpected manifest block");
            }
            block = selected.blocks().get(position++);
            if (current.recordCount() != block.recordCount) {
                throw new IOException("Manifest block record count does not match its directory");
            }
        }
        firstRecord = Math.addExact(firstRecord, current.recordCount());
        currentKey = new BlockKey(path, block.offset, block.length);
        return block;
    }

    CloseableIterator<InternalRow> rows(
            BlockKey key,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter)
            throws IOException {
        while (currentKey == null || currentKey.offset() < key.offset()) {
            if (!hasNext()) {
                throw new EOFException("Missing manifest block at " + key.offset());
            }
            next();
        }
        if (!key.equals(currentKey)) {
            throw new IOException("Unexpected manifest block position");
        }
        ManifestAvroReader.RowIterator rows =
                current.toRows(
                        ManifestEntry.MANIFEST_ROW_TYPE, partitionFilter, bucketFilter, true);
        return new CloseableIterator<InternalRow>() {

            @Override
            public boolean hasNext() {
                return rows.hasNext();
            }

            @Override
            public InternalRow next() {
                return rows.next();
            }

            @Override
            public void close() {
                // The enclosing query owns the file reader, not an individual block iterator.
            }
        };
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}
