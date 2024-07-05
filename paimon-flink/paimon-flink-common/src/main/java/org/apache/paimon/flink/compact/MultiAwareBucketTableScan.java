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

package org.apache.paimon.flink.compact;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.system.BucketsTable;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.utils.MultiTablesCompactorUtil.compactOptions;

/**
 * This class is responsible for implementing the scanning logic {@link MultiTableScanBase} for the
 * table with multi bucket such as dynamic or fixed bucket table.
 */
public class MultiAwareBucketTableScan extends MultiTableScanBase<Tuple2<Split, String>> {

    protected transient Map<Identifier, BucketsTable> tablesMap;
    protected transient Map<Identifier, StreamTableScan> scansMap;

    public MultiAwareBucketTableScan(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            boolean isStreaming,
            AtomicBoolean isRunning) {
        super(
                catalogLoader,
                includingPattern,
                excludingPattern,
                databasePattern,
                isStreaming,
                isRunning);
        tablesMap = new HashMap<>();
        scansMap = new HashMap<>();
    }

    @Override
    List<Tuple2<Split, String>> doScan() {
        List<Tuple2<Split, String>> splits = new ArrayList<>();
        for (Map.Entry<Identifier, StreamTableScan> entry : scansMap.entrySet()) {
            Identifier identifier = entry.getKey();
            StreamTableScan scan = entry.getValue();
            splits.addAll(
                    scan.plan().splits().stream()
                            .map(split -> new Tuple2<>(split, identifier.getFullName()))
                            .collect(Collectors.toList()));
        }
        return splits;
    }

    @Override
    public boolean checkTableScanned(Identifier identifier) {
        return tablesMap.containsKey(identifier);
    }

    @Override
    public void addScanTable(FileStoreTable fileStoreTable, Identifier identifier) {
        if (fileStoreTable.bucketMode() != BucketMode.BUCKET_UNAWARE) {
            BucketsTable bucketsTable =
                    new BucketsTable(fileStoreTable, isStreaming, identifier.getDatabaseName())
                            .copy(compactOptions(isStreaming));
            tablesMap.put(identifier, bucketsTable);
            scansMap.put(identifier, bucketsTable.newReadBuilder().newStreamScan());
        }
    }
}
