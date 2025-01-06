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

package org.apache.paimon.operation;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ThreadPoolUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;

/** List what data files recorded in manifests are missing from the filesystem. */
public class ListUnexistingFiles {

    private final FileStoreTable table;
    private final FileStorePathFactory pathFactory;
    private final ThreadPoolExecutor executor;

    public ListUnexistingFiles(FileStoreTable table) {
        this.table = table;
        this.pathFactory = table.store().pathFactory();
        this.executor =
                createCachedThreadPool(
                        table.coreOptions().deleteFileThreadNum(), "LIST_UNEXISTING_FILES");
    }

    public Map<Integer, Map<String, DataFileMeta>> list(BinaryRow partition) throws Exception {
        Map<Integer, Map<String, DataFileMeta>> result = new ConcurrentHashMap<>();
        List<Split> splits =
                table.newScan()
                        .withPartitionFilter(Collections.singletonList(partition))
                        .plan()
                        .splits();
        ThreadPoolUtils.randomlyOnlyExecute(
                executor, split -> listFilesInDataSplit((DataSplit) split, result), splits);
        return result;
    }

    private void listFilesInDataSplit(
            DataSplit dataSplit, Map<Integer, Map<String, DataFileMeta>> result) {
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(dataSplit.partition(), dataSplit.bucket());
        for (DataFileMeta meta : dataSplit.dataFiles()) {
            Path path = dataFilePathFactory.toPath(meta);
            try {
                if (!table.fileIO().exists(path)) {
                    result.computeIfAbsent(dataSplit.bucket(), k -> new HashMap<>())
                            .put(path.toString(), meta);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot determine if file " + path + " exists.", e);
            }
        }
    }
}
