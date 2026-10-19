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

import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataField;

import java.util.List;

/** File index factory to construct {@link FileIndexer}. */
public interface GlobalIndexerFactory {

    String identifier();

    default boolean supportsFullTextSearch() {
        return false;
    }

    /**
     * Whether a grouped index definition can serve independent scalar lookups on its fields.
     *
     * <p>Grouped fields are queried independently, unlike the ordered columns of a composite key.
     * Readers may still decline individual predicates; coverage must include only the shards that
     * actually evaluated the predicate.
     */
    default boolean supportsGroupedIndexScalarQueries() {
        return false;
    }

    /**
     * Select index files using metadata only. Implementations must retain every file that may
     * match; the default keeps all files when no safe pruning is available.
     */
    default List<GlobalIndexIOMeta> selectFiles(
            List<DataField> indexFields, Predicate predicate, List<GlobalIndexIOMeta> files) {
        return files;
    }

    /** Creates an indexer over a non-empty list of columns in index order. */
    GlobalIndexer create(List<DataField> indexFields, Options options);
}
