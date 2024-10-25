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

package org.apache.paimon.flink.compact.changelog;

import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;

/**
 * Receive and process the {@link ChangelogCompactTask}s emitted by {@link
 * ChangelogCompactCoordinateOperator}.
 */
public class ChangelogCompactWorkerOperator extends AbstractStreamOperator<Committable>
        implements OneInputStreamOperator<ChangelogCompactTask, Committable> {
    private final FileStoreTable table;

    public ChangelogCompactWorkerOperator(FileStoreTable table) {
        this.table = table;
    }

    public void processElement(StreamRecord<ChangelogCompactTask> record) throws Exception {

        ChangelogCompactTask task = record.getValue();
        List<Committable> committables = task.doCompact(table);
        committables.forEach(committable -> output.collect(new StreamRecord<>(committable)));
    }
}
