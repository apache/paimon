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

package org.apache.paimon.utils;

import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.service.ServiceManager;

/**
 * Classification of Paimon files.
 *
 * <ul>
 *   <li>{@link #META}: snapshot, schema, manifest, statistics, tag, changelog metadata, hint files,
 *       _SUCCESS, consumer, service files
 *   <li>{@link #DATA}: data files and any unrecognized files (default)
 *   <li>{@link #BUCKET_INDEX}: bucket level index files (Hash, DV)
 *   <li>{@link #GLOBAL_INDEX}: table level global index files (btree, bitmap, lumina, tantivy)
 *   <li>{@link #FILE_INDEX}: data-file index files (bloom filter, bitmap, etc.)
 * </ul>
 */
public enum FileType {
    META,
    DATA,
    BUCKET_INDEX,
    GLOBAL_INDEX,
    FILE_INDEX;

    private static final String MANIFEST = "manifest";
    private static final String CHANGELOG_DIR = "changelog";
    private static final String GLOBAL_INDEX_INFIX = "global-index-";

    /** Returns {@code true} if this file type is any kind of index. */
    public boolean isIndex() {
        return this == BUCKET_INDEX || this == GLOBAL_INDEX || this == FILE_INDEX;
    }

    /**
     * Classify a file based on its full path.
     *
     * <p>When the file does not match any known pattern, it defaults to {@link #DATA}.
     */
    public static FileType classify(Path filePath) {
        String name = filePath.getName();

        // meta file prefixes: snapshot-, schema-, stat-, tag-, consumer-, service-
        if (name.startsWith(SnapshotManager.SNAPSHOT_PREFIX)
                || name.startsWith(SchemaManager.SCHEMA_PREFIX)
                || name.startsWith(FileStorePathFactory.STATISTICS_PREFIX)
                || name.startsWith(TagManager.TAG_PREFIX)
                || name.startsWith(ConsumerManager.CONSUMER_PREFIX)
                || name.startsWith(ServiceManager.SERVICE_PREFIX)) {
            return META;
        }

        // file index: {data-file}.index (e.g. data-xxx.orc.index)
        // must check before global index since global index also ends with ".index"
        if (name.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX)) {
            if (name.contains(GLOBAL_INDEX_INFIX)) {
                return GLOBAL_INDEX;
            }
            return FILE_INDEX;
        }

        // manifest, manifest-list, index-manifest: name contains "manifest"
        if (name.contains(MANIFEST)) {
            return META;
        }

        // bucket index: name starts with "index-" (e.g. index-{uuid}-{N})
        if (name.startsWith(FileStorePathFactory.INDEX_PREFIX)) {
            return BUCKET_INDEX;
        }

        // hint files
        if ("EARLIEST".equals(name) || "LATEST".equals(name)) {
            return META;
        }

        // success files
        if ("_SUCCESS".equals(name) || name.endsWith("_SUCCESS")) {
            return META;
        }

        // changelog metadata: parent dir is "changelog" and name starts with "changelog-"
        if (name.startsWith(ChangelogManager.CHANGELOG_PREFIX)
                && CHANGELOG_DIR.equals(filePath.getParent().getName())) {
            return META;
        }

        // default: DATA
        return DATA;
    }
}
