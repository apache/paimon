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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.CoreOptions.createCommitUser;

/** This class is only used for generate compact sink topology for dynamic bucket table. */
public class DynamicBucketCompactSink extends RowDynamicBucketSink {

    public DynamicBucketCompactSink(
            FileStoreTable table, @Nullable Map<String, String> overwritePartition) {
        super(table, overwritePartition);
    }

    @Override
    public DataStreamSink<?> build(DataStream<InternalRow> input, @Nullable Integer parallelism) {
        String initialCommitUser = createCommitUser(table.coreOptions().toConfiguration());

        // This input is sorted and compacted. So there is no shuffle here, we just assign bucket
        // for each record, and sink them to table.

        // bucket-assigner
        HashBucketAssignerOperator<InternalRow> assignerOperator =
                createHashBucketAssignerOperator(
                        initialCommitUser, table, null, extractorFunction(), true);
        TupleTypeInfo<Tuple2<InternalRow, Integer>> rowWithBucketType =
                new TupleTypeInfo<>(input.getType(), BasicTypeInfo.INT_TYPE_INFO);
        DataStream<Tuple2<InternalRow, Integer>> bucketAssigned =
                input.transform("dynamic-bucket-assigner", rowWithBucketType, assignerOperator)
                        .setParallelism(input.getParallelism());
        return sinkFrom(bucketAssigned, initialCommitUser);
    }
}
