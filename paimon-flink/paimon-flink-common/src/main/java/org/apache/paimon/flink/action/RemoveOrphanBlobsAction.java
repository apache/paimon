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

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.flink.orphan.FlinkManagedBlobOrphanFilesClean.executeDatabase;
import static org.apache.paimon.flink.orphan.FlinkManagedBlobOrphanFilesClean.validateParallelism;
import static org.apache.paimon.operation.OrphanFilesClean.olderThanMillis;

/** Action to remove unreferenced primary-key managed BLOB packs. */
public class RemoveOrphanBlobsAction extends ActionBase {

    private final String databaseName;
    @Nullable private final String tableName;
    @Nullable private final Integer parallelism;

    private String olderThan = null;
    private boolean dryRun = false;

    public RemoveOrphanBlobsAction(
            String databaseName,
            @Nullable String tableName,
            @Nullable String parallelism,
            Map<String, String> catalogConfig) {
        super(catalogConfig);
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.parallelism = parseParallelism(parallelism);
    }

    public void olderThan(String olderThan) {
        this.olderThan = olderThan;
    }

    public void dryRun() {
        this.dryRun = true;
    }

    @Override
    public void run() throws Exception {
        executeDatabase(
                env,
                catalog,
                olderThanMillis(olderThan),
                dryRun,
                parallelism,
                databaseName,
                tableName);
    }

    @Nullable
    static Integer parseParallelism(@Nullable String parallelism) {
        if (parallelism == null) {
            return null;
        }

        final int parsed;
        try {
            parsed = Integer.parseInt(parallelism);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Parallelism must be a positive integer, but was '%s'.", parallelism),
                    e);
        }
        validateParallelism(parsed);
        return parsed;
    }
}
