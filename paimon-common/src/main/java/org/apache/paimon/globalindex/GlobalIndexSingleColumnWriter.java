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
 * Single-column global index writer. Range writers consume relative row IDs; centroid vector
 * writers consume absolute Paimon row IDs, as recorded in their index metadata.
 */
public interface GlobalIndexSingleColumnWriter extends GlobalIndexWriter {

    /**
     * Write the indexed key and its row ID in the writer's declared encoding.
     *
     * @param key nullable index key
     * @param rowId relative ID calculated by {@code absoluteRowId - rangeStart} for range writers,
     *     or the absolute row ID for a centroid vector writer
     */
    void write(@Nullable Object key, long rowId);

    /**
     * Finishes a writer whose normalized entries cover {@code sourceRowCount} source rows.
     *
     * <p>The default implementation preserves the one-entry-per-source-row contract. Writers for
     * zero-to-many key extraction should override this method.
     */
    default List<ResultEntry> finish(long sourceRowCount) {
        return finish();
    }
}
