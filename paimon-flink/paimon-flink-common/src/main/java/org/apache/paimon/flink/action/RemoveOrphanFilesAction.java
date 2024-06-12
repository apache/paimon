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

package org.apache.paimon.flink.action;

import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.OrphanFilesClean;
import org.apache.paimon.table.FileStoreTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Action to remove the orphan data files and metadata files. */
public class RemoveOrphanFilesAction extends TableActionBase {
    private static final Logger LOG = LoggerFactory.getLogger(RemoveOrphanFilesAction.class);

    private final OrphanFilesClean orphanFilesClean;

    public RemoveOrphanFilesAction(
            String warehouse,
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig) {
        super(warehouse, databaseName, tableName, catalogConfig);

        checkArgument(
                table instanceof FileStoreTable,
                "Only FileStoreTable supports remove-orphan-files action. The table type is '%s'.",
                table.getClass().getName());
        this.orphanFilesClean = new OrphanFilesClean((FileStoreTable) table);
    }

    public RemoveOrphanFilesAction olderThan(String timestamp) {
        this.orphanFilesClean.olderThan(timestamp);
        return this;
    }

    public RemoveOrphanFilesAction dryRun(Boolean dryRun) {
        this.orphanFilesClean.dryRun(dryRun);
        return this;
    }

    @Override
    public void run() throws Exception {
        List<Path> orphanFiles = orphanFilesClean.clean();
        String files =
                orphanFiles.stream()
                        .map(filePath -> filePath.toUri().getPath())
                        .collect(Collectors.joining(", "));
        LOG.info("orphan files: [{}]", files);
    }
}
