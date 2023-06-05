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

import org.apache.paimon.append.AppendOnlyCompactionTask;
import org.apache.paimon.append.AppendOnlyTableCompactionCoordinator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.table.AppendOnlyFileStoreTable;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/**
 * Receive scaned files and generate compaction plan for {@link
 * AppendOnlyTableCompactionWorkerOperator}.
 */
public class AppendOnlyTableCompactionCoordinatorOperator
        extends AbstractStreamOperator<AppendOnlyCompactionTask>
        implements OneInputStreamOperator<RowData, AppendOnlyCompactionTask> {
    private final AppendOnlyFileStoreTable table;
    private transient AppendOnlyTableCompactionCoordinator compactionCoordinator;
    private transient DataFileMetaSerializer dataFileMetaSerializer;

    public AppendOnlyTableCompactionCoordinatorOperator(AppendOnlyFileStoreTable table) {
        this.table = table;
    }

    @Override
    public void open() throws Exception {
        super.open();
        dataFileMetaSerializer = new DataFileMetaSerializer();
        compactionCoordinator = table.createNonBucketCompaction().getCompactionCoordinator();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        // deserialize rowData to datafile, then add it into compaction coordinator
        RowData record = element.getValue();
        BinaryRow partition = deserializeBinaryRow(record.getBinary(1));
        byte[] serializedFiles = record.getBinary(3);
        List<DataFileMeta> files = dataFileMetaSerializer.deserializeList(serializedFiles);

        // add files to compaction coordinator
        compactionCoordinator.notifyNewFiles(partition, files);

        // execute compaction check, emit compaction task to down stream
        compactionCoordinator
                .compactPlan()
                .forEach(task -> output.collect(new StreamRecord<>(task)));
    }
}
