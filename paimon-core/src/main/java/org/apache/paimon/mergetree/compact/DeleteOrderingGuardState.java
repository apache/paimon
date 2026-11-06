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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.DeleteOrderingGuardFile;
import org.apache.paimon.index.DeleteOrderingGuardReaderFactory;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Guard references corresponding to the bucket's restored and delivered compaction data. */
public class DeleteOrderingGuardState {
    private final DeleteOrderingGuardReaderFactory readerFactory;
    private final BinaryRow partition;
    private final int bucket;
    private final RowType valueType;
    private final Comparator<InternalRow> keyComparator;
    private final Set<IndexFileMeta> files = new LinkedHashSet<>();

    public DeleteOrderingGuardState(
            DeleteOrderingGuardReaderFactory readerFactory,
            BinaryRow partition,
            int bucket,
            RowType valueType,
            Comparator<InternalRow> keyComparator,
            List<IndexFileMeta> restored) {
        this.readerFactory = readerFactory;
        this.partition = partition.copy();
        this.bucket = bucket;
        this.valueType = valueType;
        this.keyComparator = keyComparator;
        this.files.addAll(restored);
    }

    public synchronized List<IndexFileMeta> files() {
        return new ArrayList<>(files);
    }

    public synchronized boolean isEmpty() {
        return files.isEmpty();
    }

    public synchronized void add(List<IndexFileMeta> additions) {
        additions.stream()
                .filter(
                        file ->
                                DeleteOrderingGuardFile.DELETE_ORDERING_GUARD.equals(
                                        file.indexType()))
                .forEach(files::add);
    }

    public synchronized void remove(List<IndexFileMeta> deletions) {
        files.removeAll(deletions);
    }

    public DeleteOrderingGuardReader open() throws IOException {
        return open(files());
    }

    public DeleteOrderingGuardReader open(List<IndexFileMeta> sources) throws IOException {
        return readerFactory.open(partition, bucket, sources, valueType, keyComparator);
    }
}
