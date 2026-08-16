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

package org.apache.paimon.flink.postpone;

import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collections;

/**
 * Operator used with {@link PostponeBucketCompactSplitSource}, to remove files in {@code bucket =
 * -2} directory.
 */
public class RemovePostponeBucketFilesOperator extends BoundedOneInputOperator<Split, Committable> {

    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(StreamRecord<Split> element) throws Exception {
        DataSplit dataSplit = (DataSplit) element.getValue();
        CommitMessageImpl message =
                new CommitMessageImpl(
                        dataSplit.partition(),
                        dataSplit.bucket(),
                        dataSplit.totalBuckets(),
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                dataSplit.dataFiles(),
                                Collections.emptyList(),
                                Collections.emptyList()));
        output.collect(new StreamRecord<>(new Committable(Long.MAX_VALUE, message)));
    }

    @Override
    public void endInput() throws Exception {}
}
