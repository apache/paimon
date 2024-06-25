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

package org.apache.paimon.index;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.VersionedObjectSerializer;

import java.util.LinkedHashMap;

/** A {@link VersionedObjectSerializer} for {@link IndexFileMeta}. */
public class IndexFileMetaSerializer extends ObjectSerializer<IndexFileMeta> {

    public IndexFileMetaSerializer() {
        super(IndexFileMeta.schema());
    }

    @Override
    public InternalRow toRow(IndexFileMeta record) {
        return GenericRow.of(
                BinaryString.fromString(record.indexType()),
                BinaryString.fromString(record.fileName()),
                record.fileSize(),
                record.rowCount(),
                record.deletionVectorsRanges() == null
                        ? null
                        : dvRangesToRowArrayData(record.deletionVectorsRanges()));
    }

    @Override
    public IndexFileMeta fromRow(InternalRow row) {
        return new IndexFileMeta(
                row.getString(0).toString(),
                row.getString(1).toString(),
                row.getLong(2),
                row.getLong(3),
                row.isNullAt(4) ? null : rowArrayDataToDvRanges(row.getArray(4)));
    }

    public static InternalArray dvRangesToRowArrayData(
            LinkedHashMap<String, Pair<Integer, Integer>> dvRanges) {
        return new GenericArray(
                dvRanges.entrySet().stream()
                        .map(
                                entry ->
                                        GenericRow.of(
                                                BinaryString.fromString(entry.getKey()),
                                                entry.getValue().getLeft(),
                                                entry.getValue().getRight()))
                        .toArray(GenericRow[]::new));
    }

    public static LinkedHashMap<String, Pair<Integer, Integer>> rowArrayDataToDvRanges(
            InternalArray arrayData) {
        LinkedHashMap<String, Pair<Integer, Integer>> dvRanges =
                new LinkedHashMap<>(arrayData.size());
        for (int i = 0; i < arrayData.size(); i++) {
            InternalRow row = arrayData.getRow(i, 3);
            dvRanges.put(row.getString(0).toString(), Pair.of(row.getInt(1), row.getInt(2)));
        }
        return dvRanges;
    }
}
