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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.store.connector.sink.global.GlobalCommitter;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.table.store.table.sink.TableCommit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** {@link GlobalCommitter} for dynamic store. */
public class StoreGlobalCommitter implements GlobalCommitter<Committable, ManifestCommittable> {

    private final TableCommit commit;

    @Nullable private final CatalogLock lock;

    public StoreGlobalCommitter(TableCommit commit, @Nullable CatalogLock lock) {
        this.commit = commit;
        this.lock = lock;
    }

    @Override
    public void close() throws Exception {
        if (lock != null) {
            lock.close();
        }
    }

    @Override
    public List<ManifestCommittable> filterRecoveredCommittables(
            List<ManifestCommittable> globalCommittables) {
        return commit.filterCommitted(globalCommittables);
    }

    @Override
    public ManifestCommittable combine(long checkpointId, List<Committable> committables)
            throws IOException {
        ManifestCommittable fileCommittable = new ManifestCommittable(String.valueOf(checkpointId));
        for (Committable committable : committables) {
            switch (committable.kind()) {
                case FILE:
                    FileCommittable file = (FileCommittable) committable.wrappedCommittable();
                    fileCommittable.addFileCommittable(
                            file.partition(), file.bucket(), file.increment());
                    break;
                case LOG_OFFSET:
                    LogOffsetCommittable offset =
                            (LogOffsetCommittable) committable.wrappedCommittable();
                    fileCommittable.addLogOffset(offset.bucket(), offset.offset());
                    break;
                case LOG:
                    // log should be committed in local committer
                    break;
            }
        }
        return fileCommittable;
    }

    @Override
    public void commit(List<ManifestCommittable> committables)
            throws IOException, InterruptedException {
        commit.commit(committables);
    }
}
