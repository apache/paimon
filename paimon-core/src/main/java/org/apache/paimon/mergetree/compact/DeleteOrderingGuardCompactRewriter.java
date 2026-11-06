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

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.index.DeleteOrderingGuardFile;
import org.apache.paimon.index.DeleteOrderingGuardSerializer;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Owns guard output while the native rewriter preserves data and changelog semantics. */
class DeleteOrderingGuardCompactRewriter implements CompactRewriter {

    private final CompactRewriter wrapped;
    private final DeleteOrderingGuardSerializer serializer;
    private final DeleteOrderingGuardFile guardFile;
    // Retain ownership until the manager delivers the result, including cancelled futures.
    private final Set<IndexFileMeta> pending = new HashSet<>();
    @Nullable private DeleteOrderingGuardFile.Writer activeWriter;
    private boolean closed;

    DeleteOrderingGuardCompactRewriter(
            CompactRewriter wrapped,
            DeleteOrderingGuardSerializer serializer,
            DeleteOrderingGuardFile guardFile) {
        this.wrapped = wrapped;
        this.serializer = serializer;
        this.guardFile = guardFile;
    }

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        return capture(() -> wrapped.rewrite(outputLevel, dropDelete, sections));
    }

    @Override
    public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
        return capture(() -> wrapped.upgrade(outputLevel, file));
    }

    <T> MergeFunctionWrapper<T> wrap(MergeFunctionWrapper<T> mergeFunction) {
        checkState(activeWriter != null, "Guard capture requires an active compaction rewrite.");
        return new DeleteOrderingGuardMergeFunctionWrapper<>(
                mergeFunction, serializer, activeWriter::write);
    }

    private CompactResult capture(Callable<CompactResult> action) throws Exception {
        checkState(activeWriter == null, "Concurrent guard capture is not supported.");
        synchronized (this) {
            checkState(!closed, "Delete ordering guard rewriter is closed.");
        }
        try (DeleteOrderingGuardFile.Writer writer = guardFile.createWriter()) {
            activeWriter = writer;
            CompactResult result = null;
            try {
                result = action.call();
                IndexFileMeta guard = writer.finish();
                if (guard != null) {
                    synchronized (this) {
                        if (closed) {
                            throw new IOException("Delete ordering guard rewriter is closed.");
                        }
                        result.newIndexFiles().add(guard);
                        pending.add(guard);
                    }
                }
                return result;
            } catch (Exception | Error failure) {
                try {
                    writer.abort();
                } catch (Throwable cleanupFailure) {
                    if (cleanupFailure != failure) {
                        failure.addSuppressed(cleanupFailure);
                    }
                }
                if (result != null) {
                    try {
                        abort(result);
                    } catch (Throwable cleanupFailure) {
                        if (cleanupFailure != failure) {
                            failure.addSuppressed(cleanupFailure);
                        }
                    }
                }
                throw failure;
            }
        } finally {
            activeWriter = null;
        }
    }

    @Override
    public void abort(CompactResult result) {
        try {
            wrapped.abort(result);
        } finally {
            synchronized (this) {
                pending.removeAll(result.newIndexFiles());
            }
        }
    }

    @Override
    public synchronized void release(CompactResult result) {
        pending.removeAll(result.newIndexFiles());
        wrapped.release(result);
    }

    @Override
    public void close() throws IOException {
        List<IndexFileMeta> toDelete;
        synchronized (this) {
            closed = true;
            toDelete = new ArrayList<>(pending);
            pending.clear();
        }
        try {
            wrapped.close();
        } finally {
            toDelete.forEach(guardFile::delete);
        }
    }
}
