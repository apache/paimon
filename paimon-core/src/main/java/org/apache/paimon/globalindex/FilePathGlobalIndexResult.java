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

package org.apache.paimon.globalindex;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Result of a BTree-with-file-meta global index query. Contains pre-fetched {@link
 * org.apache.paimon.manifest.ManifestEntry} bytes (from the btree_file_meta index) that can be used
 * to build a query plan without reading manifest files from HDFS, plus an optional row-level {@link
 * GlobalIndexResult} (from the btree key index) for fine-grained row filtering inside matching
 * files.
 */
public class FilePathGlobalIndexResult {

    private final List<byte[]> manifestEntryBytes;
    @Nullable private final GlobalIndexResult rowIndexResult;

    public FilePathGlobalIndexResult(
            List<byte[]> manifestEntryBytes, @Nullable GlobalIndexResult rowIndexResult) {
        this.manifestEntryBytes = manifestEntryBytes;
        this.rowIndexResult = rowIndexResult;
    }

    /**
     * Raw serialized {@link org.apache.paimon.manifest.ManifestEntry} bytes from btree_file_meta
     * index.
     */
    public List<byte[]> manifestEntryBytes() {
        return manifestEntryBytes;
    }

    /** Whether row-level filtering is available via the btree key index. */
    public boolean hasRowLevelFilter() {
        return rowIndexResult != null;
    }

    /** The row-level index result (btree key index), or {@code null} if not available. */
    @Nullable
    public GlobalIndexResult rowIndexResult() {
        return rowIndexResult;
    }
}
