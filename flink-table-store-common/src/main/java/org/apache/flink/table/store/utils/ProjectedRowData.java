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

package org.apache.flink.table.store.utils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;

/**
 * An implementation of {@link MappingRowData} which returns true in {@link #isNullAt} when mapping
 * index is negative.
 */
public class ProjectedRowData extends MappingRowData {

    private ProjectedRowData(int[] indexMapping) {
        super(indexMapping);
    }

    /**
     * Replaces the underlying {@link RowData} backing this {@link ProjectedRowData}.
     *
     * <p>This method replaces the row data in place and does not return a new object. This is done
     * for performance reasons.
     */
    public ProjectedRowData replaceRow(RowData row) {
        this.row = row;
        return this;
    }

    @Override
    public boolean isNullAt(int pos) {
        if (indexMapping[pos] < 0) {
            return true;
        }
        return super.isNullAt(pos);
    }

    /**
     * Like {@link #from(int[])}, but throws {@link IllegalArgumentException} if the provided {@code
     * projection} array contains nested projections, which are not supported by {@link
     * ProjectedRowData}.
     *
     * <p>The array represents the mapping of the fields of the original {@link DataType}, including
     * nested rows. For example, {@code [[0, 2, 1], ...]} specifies to include the 2nd field of the
     * 3rd field of the 1st field in the top-level row.
     *
     * @see Projection
     * @see ProjectedRowData
     */
    public static ProjectedRowData from(int[][] projection) throws IllegalArgumentException {
        return new ProjectedRowData(
                Arrays.stream(projection)
                        .mapToInt(
                                arr -> {
                                    if (arr.length != 1) {
                                        throw new IllegalArgumentException(
                                                "ProjectedRowData doesn't support nested projections");
                                    }
                                    return arr[0];
                                })
                        .toArray());
    }

    /**
     * Create an empty {@link ProjectedRowData} starting from a {@code projection} array.
     *
     * <p>The array represents the mapping of the fields of the original {@link DataType}. For
     * example, {@code [0, 2, 1]} specifies to include in the following order the 1st field, the 3rd
     * field and the 2nd field of the row.
     *
     * @see Projection
     * @see ProjectedRowData
     */
    public static ProjectedRowData from(int[] projection) {
        return new ProjectedRowData(projection);
    }

    /**
     * Create an empty {@link ProjectedRowData} starting from a {@link Projection}.
     *
     * <p>Throws {@link IllegalStateException} if the provided {@code projection} array contains
     * nested projections, which are not supported by {@link ProjectedRowData}.
     *
     * @see Projection
     * @see ProjectedRowData
     */
    public static ProjectedRowData from(Projection projection) {
        return new ProjectedRowData(projection.toTopLevelIndexes());
    }
}
