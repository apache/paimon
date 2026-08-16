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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Parsed file-level metadata for one shared-shredding MAP column. */
public class MapSharedShreddingFieldMeta {

    private final Map<String, Integer> nameToId;
    private final Map<Integer, List<Integer>> fieldToColumns;
    private final Set<Integer> overflowFieldSet;
    private final int numColumns;
    private final int maxRowWidth;

    public MapSharedShreddingFieldMeta(
            Map<String, Integer> nameToId,
            Map<Integer, List<Integer>> fieldToColumns,
            Set<Integer> overflowFieldSet,
            int numColumns,
            int maxRowWidth) {
        this.nameToId = nameToId;
        this.fieldToColumns = fieldToColumns;
        this.overflowFieldSet = overflowFieldSet;
        this.numColumns = numColumns;
        this.maxRowWidth = maxRowWidth;
    }

    public Map<String, Integer> nameToId() {
        return nameToId;
    }

    public Map<Integer, List<Integer>> fieldToColumns() {
        return fieldToColumns;
    }

    public Set<Integer> overflowFieldSet() {
        return overflowFieldSet;
    }

    public int numColumns() {
        return numColumns;
    }

    public int maxRowWidth() {
        return maxRowWidth;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MapSharedShreddingFieldMeta)) {
            return false;
        }
        MapSharedShreddingFieldMeta that = (MapSharedShreddingFieldMeta) o;
        return numColumns == that.numColumns
                && maxRowWidth == that.maxRowWidth
                && Objects.equals(nameToId, that.nameToId)
                && Objects.equals(fieldToColumns, that.fieldToColumns)
                && Objects.equals(overflowFieldSet, that.overflowFieldSet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nameToId, fieldToColumns, overflowFieldSet, numColumns, maxRowWidth);
    }
}
