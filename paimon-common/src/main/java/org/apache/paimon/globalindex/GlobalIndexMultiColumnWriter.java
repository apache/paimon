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

import org.apache.paimon.data.InternalRow;

import javax.annotation.Nullable;

/** Index writer for global index that accepts multiple column values per row. */
public interface GlobalIndexMultiColumnWriter extends GlobalIndexWriter {

    /**
     * Write one record's indexed columns at the given relative row id.
     *
     * @param rowId the record's row id relative to the current shard (0 to rowCnt - 1); a null row
     *     still advances the row id without indexing a value
     * @param row a projected row containing only the indexed columns, whose layout matches the
     *     fields order passed to {@link GlobalIndexerFactory#create(java.util.List,
     *     org.apache.paimon.options.Options)}
     */
    void write(long rowId, @Nullable InternalRow row);
}
