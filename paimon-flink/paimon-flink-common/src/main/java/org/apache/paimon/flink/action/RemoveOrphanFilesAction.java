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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.orphan.CombinedFlinkOrphanFilesClean;
import org.apache.paimon.flink.orphan.FlinkOrphanFilesClean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.apache.paimon.operation.OrphanFilesClean.olderThanMillis;

/** Action to remove the orphan data files and metadata files. */
public class RemoveOrphanFilesAction extends ActionBase {

    protected static final Logger LOG = LoggerFactory.getLogger(RemoveOrphanFilesAction.class);

    private final String databaseName;
    @Nullable private final List<Identifier> tableIdentifiers;
    @Nullable private final String parallelism;

    private String olderThan = null;
    private boolean dryRun = false;
    private MultiTablesSinkMode mode = COMBINED;

    public RemoveOrphanFilesAction(
            String databaseName,
            @Nullable String tableName,
            @Nullable String parallelism,
            Map<String, String> catalogConfig) {
        super(catalogConfig);
        this.databaseName = databaseName;
        this.tableIdentifiers =
                Objects.isNull(tableName)
                        ? new ArrayList<>()
                        : Collections.singletonList(Identifier.create(databaseName, tableName));
        this.parallelism = parallelism;
    }

    public RemoveOrphanFilesAction(
            String databaseName,
            List<Identifier> tableIdentifiers,
            @Nullable String parallelism,
            Map<String, String> catalogConfig) {
        super(catalogConfig);
        this.databaseName = databaseName;
        this.tableIdentifiers =
                Objects.nonNull(tableIdentifiers) ? tableIdentifiers : new ArrayList<>();
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
        List<Identifier> effectiveTableIdentifiers = tableIdentifiers;
        if (effectiveTableIdentifiers == null
                || effectiveTableIdentifiers.isEmpty()
                || (effectiveTableIdentifiers.size() == 1
                        && "*".equals(effectiveTableIdentifiers.get(0).getTableName()))) {
            if (Objects.isNull(databaseName)) {
                LOG.warn("databaseName is null, will skip orphan files clean.");
                return;
            }
            effectiveTableIdentifiers =
                    catalog.listTables(databaseName).stream()
                            .map(table -> Identifier.create(databaseName, table))
                            .collect(Collectors.toList());
        }
        if (COMBINED.equals(mode)) {
            CombinedFlinkOrphanFilesClean.executeDatabaseOrphanFiles(
                    env,
                    catalog,
                    olderThanMillis(olderThan),
                    dryRun,
                    parallelism == null ? null : Integer.parseInt(parallelism),
                    effectiveTableIdentifiers);
        } else {
            FlinkOrphanFilesClean.executeDatabaseOrphanFiles(
                    env,
                    catalog,
                    olderThanMillis(olderThan),
                    dryRun,
                    parallelism == null ? null : Integer.parseInt(parallelism),
                    effectiveTableIdentifiers);
        }
    }
}
