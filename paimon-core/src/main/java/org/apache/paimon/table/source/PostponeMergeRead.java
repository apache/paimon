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

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** Reads execution-engine-routed inputs with Paimon's merge semantics. */
public final class PostponeMergeRead {

    private final FileStoreTable table;
    @Nullable private final Predicate filter;
    private final RowType resultReadType;
    private final RowType mergeReadType;

    @Nullable private IOManager ioManager;

    PostponeMergeRead(
            FileStoreTable table,
            @Nullable Predicate filter,
            RowType resultReadType,
            RowType mergeReadType) {
        this.table = table;
        this.filter = filter;
        this.resultReadType = resultReadType;
        this.mergeReadType = mergeReadType;
    }

    public PostponeMergeRead withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    /** Reads one physical postpone file with its plan-assigned relative replay sequence. */
    public RecordReader<KeyValue> createPostponeFileReader(PostponeFileReadTask task)
            throws IOException {
        return newMergeRead(mergeReadType).createPostponeReader(task);
    }

    /** Merges all real splits and routed postpone records for one target bucket. */
    public RecordReader<InternalRow> createBucketMergeReader(
            List<DataSplit> realSplits, RecordReader<KeyValue> bucketPostponeRecords)
            throws IOException {
        RecordReader<KeyValue> reader =
                newMergeRead(resultReadType)
                        .createPostponeMergeReader(realSplits, bucketPostponeRecords);
        return KeyValueTableRead.unwrap(reader, table.schema().options());
    }

    private MergeFileSplitRead newMergeRead(RowType readType) {
        MergeFileSplitRead read =
                ((KeyValueFileStore) table.store()).newRead().withReadType(readType);
        if (filter != null) {
            read.withFilter(filter);
        }
        if (ioManager != null) {
            read.withIOManager(ioManager);
        }
        return read;
    }
}
