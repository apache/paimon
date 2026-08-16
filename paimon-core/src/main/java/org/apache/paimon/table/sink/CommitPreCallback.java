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

package org.apache.paimon.table.sink;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;

import java.util.List;

/**
 * Callback which is invoked before a snapshot is committed.
 *
 * <p>This hook allows additional validation or bookkeeping to be performed before a snapshot
 * becomes visible. Implementations can inspect the files that are about to be committed as well as
 * the {@link Snapshot} metadata and decide whether the commit should proceed.
 *
 * <p>If {@link #call(List, List, List, Snapshot)} throws a {@link RuntimeException}, the commit is
 * aborted and the snapshot will not be committed. Implementations are expected to be fast and
 * either side effect free or idempotent, because a single logical commit may be retried and this
 * callback might therefore be invoked multiple times for the same logical changes.
 *
 * <p>Implementations may optionally override {@link AutoCloseable#close()} if they hold external
 * resources that need to be released when the surrounding {@link org.apache.paimon.table.Table}
 * commit is closed.
 */
public interface CommitPreCallback extends AutoCloseable {

    void call(
            List<SimpleFileEntry> baseFiles,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot);
}
