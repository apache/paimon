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

package org.apache.paimon.index;

import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.IntHashSet;
import org.apache.paimon.utils.IntIterator;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** An {@link IndexMaintainer} for dynamic bucket to maintain key hashcode in a bucket. */
public class HashIndexMaintainer implements IndexMaintainer<KeyValue> {

    private final IndexFileHandler fileHandler;
    private final IntHashSet hashcode;

    private boolean modified;

    private HashIndexMaintainer(
            IndexFileHandler fileHandler,
            @Nullable Long snapshotId,
            BinaryRow partition,
            int bucket) {
        this.fileHandler = fileHandler;
        IntHashSet hashcode = new IntHashSet();
        if (snapshotId != null) {
            Optional<IndexFileMeta> indexFile =
                    fileHandler.scanHashIndex(snapshotId, partition, bucket);

            if (indexFile.isPresent()) {
                IndexFileMeta file = indexFile.get();
                hashcode = new IntHashSet((int) file.rowCount());
                restore(fileHandler, hashcode, file);
            }
        }
        this.hashcode = hashcode;
        this.modified = false;
    }

    private void restore(IndexFileHandler fileHandler, IntHashSet hashcode, IndexFileMeta file) {
        try (IntIterator iterator = fileHandler.readHashIndex(file)) {
            while (true) {
                try {
                    hashcode.add(iterator.next());
                } catch (EOFException ignored) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void notifyNewRecord(KeyValue record) {
        InternalRow key = record.key();
        if (!(key instanceof BinaryRow)) {
            throw new IllegalArgumentException("Unsupported key type: " + key.getClass());
        }
        boolean changed = hashcode.add(key.hashCode());
        if (changed) {
            modified = true;
        }
    }

    @Override
    public List<IndexFileMeta> prepareCommit() {
        if (modified) {
            IndexFileMeta entry =
                    fileHandler.writeHashIndex(hashcode.size(), hashcode.toIntIterator());
            modified = false;
            return Collections.singletonList(entry);
        }
        return Collections.emptyList();
    }

    @VisibleForTesting
    public boolean isEmpty() {
        return hashcode.size() == 0;
    }

    /** Factory to restore {@link HashIndexMaintainer}. */
    public static class Factory implements IndexMaintainer.Factory<KeyValue> {

        private final IndexFileHandler handler;

        public Factory(IndexFileHandler handler) {
            this.handler = handler;
        }

        @Override
        public IndexMaintainer<KeyValue> createOrRestore(
                @Nullable Long snapshotId, BinaryRow partition, int bucket) {
            return new HashIndexMaintainer(handler, snapshotId, partition, bucket);
        }
    }
}
