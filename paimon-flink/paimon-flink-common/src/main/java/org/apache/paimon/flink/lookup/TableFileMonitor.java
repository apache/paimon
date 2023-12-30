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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.SCAN_BOUNDED_WATERMARK;
import static org.apache.paimon.CoreOptions.STREAM_SCAN_MODE;
import static org.apache.paimon.CoreOptions.StreamScanMode.FILE_MONITOR;

/** A file monitor of the table. */
public class TableFileMonitor {

    private final StreamTableScan scan;

    public TableFileMonitor(Table dimTable, @Nullable Predicate predicate) {
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(STREAM_SCAN_MODE.key(), FILE_MONITOR.getValue());
        dynamicOptions.put(SCAN_BOUNDED_WATERMARK.key(), null);
        FileStoreTable table = (FileStoreTable) dimTable.copy(dynamicOptions);

        // todo support key filters
        this.scan = table.newReadBuilder().newStreamScan();
    }

    public List<Split> readChanges() {
        List<Split> changes = new ArrayList<>();
        // append
        changes.addAll(scan.plan().splits());
        // compact
        changes.addAll(scan.plan().splits());
        return changes;
    }
}
