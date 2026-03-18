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

package org.apache.paimon.table.source;

import org.apache.paimon.annotation.Public;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * An input split for reading.
 *
 * @since 0.4.0
 */
@Public
public interface Split extends Serializable {

    /**
     * The row count in files, may be duplicated, such as in the primary key table and the
     * Data-Evolution Append table.
     */
    long rowCount();

    /**
     * Return the merged row count of data files. For example, when the delete vector is enabled in
     * the primary key table, the number of rows that have been deleted will be subtracted from the
     * returned result. In the Data Evolution mode of the Append table, the actual number of rows
     * will be returned.
     */
    OptionalLong mergedRowCount();

    /**
     * If all files in this split can be read without merging, returns an {@link Optional} wrapping
     * a list of {@link RawFile}s to be read without merging. Otherwise, returns {@link
     * Optional#empty()}.
     */
    default Optional<List<RawFile>> convertToRawFiles() {
        return Optional.empty();
    }

    /**
     * Return the deletion file of the data file, indicating which row in the data file was deleted.
     *
     * <p>If there is no corresponding deletion file, the element will be null.
     */
    default Optional<List<DeletionFile>> deletionFiles() {
        return Optional.empty();
    }

    /**
     * * Return the index file of the data file, for example, bloom-filter index. All the type of
     * indexes and columns will be stored in one single index file.
     *
     * <p>If there is no corresponding index file, the element will be null.
     */
    default Optional<List<IndexFile>> indexFiles() {
        return Optional.empty();
    }
}
