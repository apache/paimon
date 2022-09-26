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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.TableCompact;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** A dedicated operator for manual triggered compaction. */
public class StoreCompactOperator extends PrepareCommitOperator {

    private final FileStoreTable table;

    @Nullable private final Map<String, String> compactPartitionSpec;

    private TableCompact compact;

    public StoreCompactOperator(
            FileStoreTable table, @Nullable Map<String, String> compactPartitionSpec) {
        this.table = table;
        this.compactPartitionSpec = compactPartitionSpec;
    }

    @Override
    public void open() throws Exception {
        super.open();
        int task = getRuntimeContext().getIndexOfThisSubtask();
        int numTask = getRuntimeContext().getNumberOfParallelSubtasks();
        compact = table.newCompact();
        compact.withPartitions(
                compactPartitionSpec == null ? Collections.emptyMap() : compactPartitionSpec);
        compact.withFilter(
                (partition, bucket) -> task == Math.abs(Objects.hash(partition, bucket) % numTask));
    }

    @Override
    protected List<Committable> prepareCommit(boolean endOfInput, long checkpointId)
            throws IOException {
        return compact.compact().stream()
                .map(c -> new Committable(checkpointId, Committable.Kind.FILE, c))
                .collect(Collectors.toList());
    }
}
