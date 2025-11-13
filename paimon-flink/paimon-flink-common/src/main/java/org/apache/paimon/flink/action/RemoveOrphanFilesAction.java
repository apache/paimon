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

import org.apache.paimon.flink.orphan.CombinedFlinkOrphanFilesClean;
import org.apache.paimon.flink.orphan.FlinkOrphanFilesClean;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.apache.paimon.flink.action.MultiTablesSinkMode.DIVIDED;
import static org.apache.paimon.operation.OrphanFilesClean.olderThanMillis;

/** Action to remove the orphan data files and metadata files. */
public class RemoveOrphanFilesAction extends ActionBase {

    private final String databaseName;
    @Nullable private final List<String> tableNames;
    @Nullable private final String parallelism;

    private String olderThan = null;
    private boolean dryRun = false;
    private MultiTablesSinkMode mode = DIVIDED;

    public RemoveOrphanFilesAction(
            String databaseName,
            @Nullable List<String> tableNames,
            @Nullable String parallelism,
            Map<String, String> catalogConfig) {
        super(catalogConfig);
        this.databaseName = databaseName;
        this.tableNames = tableNames;
        this.parallelism = parallelism;
    }

    public void olderThan(String olderThan) {
        this.olderThan = olderThan;
    }

    public void dryRun() {
        this.dryRun = true;
    }

    public void mode(MultiTablesSinkMode mode) {
        this.mode = mode;
    }

    @Override
    public void run() throws Exception {
        List<String> effectiveTableNames = tableNames;
        if (effectiveTableNames == null
                || effectiveTableNames.isEmpty()
                || (tableNames.size() == 1 && "*".equals(tableNames.get(0)))) {
            effectiveTableNames = catalog.listTables(databaseName);
        }
        if (COMBINED.equals(mode)) {
            CombinedFlinkOrphanFilesClean.executeDatabaseOrphanFiles(
                    env,
                    catalog,
                    olderThanMillis(olderThan),
                    dryRun,
                    parallelism == null ? null : Integer.parseInt(parallelism),
                    databaseName,
                    effectiveTableNames);
        } else {
            FlinkOrphanFilesClean.executeDatabaseOrphanFiles(
                    env,
                    catalog,
                    olderThanMillis(olderThan),
                    dryRun,
                    parallelism == null ? null : Integer.parseInt(parallelism),
                    databaseName,
                    effectiveTableNames);
        }
    }
}
