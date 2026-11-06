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
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.mergetree.compact.DeleteOrderingGuardReader;
import org.apache.paimon.mergetree.compact.DeleteOrderingGuardState;
import org.apache.paimon.utils.FieldsComparator;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Streams guard replacements, retaining ownership until a compaction result is delivered. */
public class DeleteOrderingGuardConsolidator implements Closeable {

    private final DeleteOrderingGuardState state;
    private final DeleteOrderingGuardSerializer serializer;
    private final DeleteOrderingGuardFile file;
    private final Comparator<KeyValue> sequenceComparator;
    private final int minFileNum;
    private final Set<IndexFileMeta> pending = new HashSet<>();
    private boolean closed;

    public DeleteOrderingGuardConsolidator(
            DeleteOrderingGuardState state,
            DeleteOrderingGuardSerializer serializer,
            DeleteOrderingGuardFile file,
            @Nullable FieldsComparator sourceComparator,
            int minFileNum) {
        this.state = state;
        this.serializer = serializer;
        this.file = file;
        this.sequenceComparator = DeleteOrderingGuardReader.sequenceComparator(sourceComparator);
        this.minFileNum = minFileNum;
    }

    public boolean shouldConsolidate(boolean fullCompaction, List<IndexFileMeta> sources) {
        return sources.size() >= (fullCompaction ? 2 : minFileNum);
    }

    public CompactResult rewrite(List<IndexFileMeta> sources) throws IOException {
        synchronized (this) {
            checkState(!closed, "Delete ordering guard consolidator is closed.");
        }
        CompactResult result = null;
        try (DeleteOrderingGuardReader reader = state.open(sources);
                DeleteOrderingGuardFile.Writer writer = file.createWriter()) {
            IndexFileMeta replacement = null;
            try {
                List<KeyValue> bounds;
                while ((bounds = reader.nextKey()) != null) {
                    // This bound dominates every older retract in native source/internal order.
                    KeyValue strongest = bounds.get(0);
                    for (int i = 1; i < bounds.size(); i++) {
                        if (sequenceComparator.compare(bounds.get(i), strongest) > 0) {
                            strongest = bounds.get(i);
                        }
                    }
                    writer.write(extract(strongest));
                }
                replacement = writer.finish();
                checkState(replacement != null, "Guard consolidation cannot discard all bounds.");
                result = new CompactResult();
                result.deletedIndexFiles().addAll(sources);
                result.newIndexFiles().add(replacement);
                synchronized (this) {
                    checkState(!closed, "Delete ordering guard consolidator is closed.");
                    pending.add(replacement);
                }
                return result;
            } catch (IOException | RuntimeException | Error failure) {
                if (replacement != null) {
                    synchronized (this) {
                        pending.remove(replacement);
                    }
                }
                try {
                    writer.abort();
                } catch (Throwable cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
        } catch (IOException | RuntimeException | Error failure) {
            // A reader-close failure also cancels publication of an otherwise finished segment.
            if (result != null) {
                abort(result);
            }
            throw failure;
        }
    }

    private DeleteOrderingGuard extract(KeyValue bound) {
        DeleteOrderingGuard guard = serializer.extract(bound);
        checkState(guard != null, "Guard consolidation must preserve explicit retract metadata.");
        return guard;
    }

    public synchronized void release(CompactResult result) {
        pending.removeAll(result.newIndexFiles());
    }

    public synchronized void abort(CompactResult result) {
        for (IndexFileMeta output : result.newIndexFiles()) {
            if (pending.remove(output)) {
                file.delete(output);
            }
        }
    }

    @Override
    public void close() {
        List<IndexFileMeta> outputs;
        synchronized (this) {
            closed = true;
            outputs = new ArrayList<>(pending);
            pending.clear();
        }
        outputs.forEach(file::delete);
    }
}
