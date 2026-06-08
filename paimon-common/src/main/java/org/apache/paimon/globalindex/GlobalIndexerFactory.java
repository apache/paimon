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
import org.apache.paimon.types.DataField;

import java.util.List;

/** File index factory to construct {@link FileIndexer}. */
public interface GlobalIndexerFactory {

    String identifier();

    GlobalIndexer create(DataField dataField, Options options);

    /**
     * Whether this index type supports multi-column indexes. A factory that returns {@code true}
     * must override {@link #create(DataField, List, Options)} to handle extra columns.
     */
    default boolean supportsMultiColumn() {
        return false;
    }

    /**
     * Creates an indexer over a primary column plus optional extra columns. {@code dataField} is
     * the primary column; {@code extraFields} holds the remaining columns and is empty for a
     * single-column index.
     */
    default GlobalIndexer create(
            DataField dataField, List<DataField> extraFields, Options options) {
        if (extraFields != null && !extraFields.isEmpty()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Index type '%s' does not support multi-column index, got extra columns: %s",
                            identifier(), extraFields));
        }
        return create(dataField, options);
    }
}
