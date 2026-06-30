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

package org.apache.paimon.data.shredding;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import java.util.Collections;
import java.util.Map;

/**
 * A per-file physical layout plan for shredded fields.
 *
 * <p>The logical row type is the table-visible schema. The physical row type is the schema written
 * into the data file. Implementations convert logical rows to physical rows before handing them to
 * a file format writer.
 */
public interface ShreddingWritePlan {

    RowType logicalRowType();

    RowType physicalRowType();

    InternalRow toPhysicalRow(InternalRow row);

    /**
     * Returns top-level field metadata to persist with the physical row type.
     *
     * <p>The outer map is keyed by physical top-level field name. The inner map contains metadata
     * for that field. Formats decide how to encode this metadata into the actual file footer.
     */
    default Map<String, Map<String, String>> fieldMetadata(String compression) {
        return Collections.emptyMap();
    }
}
