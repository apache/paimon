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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.columnar.writable.WritableIntVector;

import org.apache.parquet.column.Dictionary;

/**
 * Interface to update a vector with values read from a Parquet file.
 *
 * @param <T> type of the vector to update
 */
public interface ParquetVectorUpdater<T extends WritableColumnVector> {
    /**
     * Read a batch of `total` values from `valuesReader` into `values`, starting from `offset`.
     *
     * @param total total number of values to read
     * @param offset starting offset in `values`
     * @param values destination values vector
     * @param valuesReader reader to read values from
     */
    void readValues(int total, int offset, T values, VectorizedValuesReader valuesReader);

    /**
     * Skip a batch of `total` values from `valuesReader`.
     *
     * @param total total number of values to skip
     * @param valuesReader reader to skip values from
     */
    void skipValues(int total, VectorizedValuesReader valuesReader);

    /**
     * Read a single value from `valuesReader` into `values`, at `offset`.
     *
     * @param offset offset in `values` to put the new value
     * @param values destination value vector
     * @param valuesReader reader to read values from
     */
    void readValue(int offset, T values, VectorizedValuesReader valuesReader);

    /**
     * Process a batch of `total` values starting from `offset` in `values`, whose null slots should
     * have already been filled, and fills the non-null slots using dictionary IDs from
     * `dictionaryIds`, together with Parquet `dictionary`.
     *
     * @param total total number slots to process in `values`
     * @param offset starting offset in `values`
     * @param values destination value vector
     * @param dictionaryIds vector storing the dictionary IDs
     * @param dictionary Parquet dictionary used to decode a dictionary ID to its value
     */
    default void decodeDictionaryIds(
            int total,
            int offset,
            T values,
            WritableIntVector dictionaryIds,
            Dictionary dictionary) {
        for (int i = offset; i < offset + total; i++) {
            if (!values.isNullAt(i)) {
                decodeSingleDictionaryId(i, values, dictionaryIds, dictionary);
            }
        }
    }

    /**
     * Decode a single dictionary ID from `dictionaryIds` into `values` at `offset`, using
     * `dictionary`.
     *
     * @param offset offset in `values` to put the decoded value
     * @param values destination value vector
     * @param dictionaryIds vector storing the dictionary IDs
     * @param dictionary Parquet dictionary used to decode a dictionary ID to its value
     */
    void decodeSingleDictionaryId(
            int offset, T values, WritableIntVector dictionaryIds, Dictionary dictionary);
}
