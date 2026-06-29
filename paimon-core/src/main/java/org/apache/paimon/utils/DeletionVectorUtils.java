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

import org.apache.paimon.io.DataFileMeta;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Util class for Deletion Vectors. */
public class DeletionVectorUtils {

    /**
     * Retrieve the anchor file of a row range group. Always the oldest normal file. Note that in
     * current framework, it's impossible to have multiple normal files with same max_seq_num.
     */
    public static <T> T retrieveAnchorFile(
            Collection<T> entries, Function<T, DataFileMeta> fileMetaFunc) {
        T anchor = null;
        long min = Long.MAX_VALUE;
        Set<Long> maxSequenceNumbers = new HashSet<>();
        for (T entry : entries) {
            DataFileMeta meta = fileMetaFunc.apply(entry);
            if (isBlobFile(meta.fileName()) || isVectorStoreFile(meta.fileName())) {
                continue;
            }

            long maxSequenceNumber = meta.maxSequenceNumber();
            checkState(
                    maxSequenceNumbers.add(maxSequenceNumber),
                    "More than one normal data file has the same max sequence number %s "
                            + "in a data-evolution row range group.",
                    maxSequenceNumber);
            if (maxSequenceNumber < min) {
                anchor = entry;
                min = maxSequenceNumber;
            }
        }
        checkState(
                anchor != null,
                "Data-evolution deletion vectors should have a normal anchor file in each row range group.");
        return anchor;
    }
}
