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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** List what data files recorded in manifests are missing from the filesystem. */
public class ListUnexistingFiles {

    private final FileStoreTable table;

    public ListUnexistingFiles(FileStoreTable table) {
        this.table = table;
    }

    public Map<Integer, Map<String, DataFileMeta>> list(BinaryRow partition) throws Exception {
        FileIO fileIO = table.fileIO();
        FileStorePathFactory pathFactory = table.store().pathFactory();
        Map<Integer, Map<String, DataFileMeta>> result = new HashMap<>();
        List<Split> splits =
                table.newScan()
                        .withPartitionFilter(Collections.singletonList(partition))
                        .plan()
                        .splits();
        for (Split split : splits) {
            DataSplit dataSplit = (DataSplit) split;
            DataFilePathFactory dataFilePathFactory =
                    pathFactory.createDataFilePathFactory(partition, dataSplit.bucket());
            for (DataFileMeta meta : dataSplit.dataFiles()) {
                Path path = dataFilePathFactory.toPath(meta);
                if (!fileIO.exists(path)) {
                    result.computeIfAbsent(dataSplit.bucket(), k -> new HashMap<>())
                            .put(path.toString(), meta);
                }
            }
        }
        return result;
    }
}
