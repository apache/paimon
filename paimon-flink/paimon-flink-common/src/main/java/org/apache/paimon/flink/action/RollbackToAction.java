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

import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** Rollback to specific version action for Flink. */
public class RollbackToAction extends TableActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(RollbackToAction.class);

    private final String version;

    private final Boolean isTimestamp;

    public RollbackToAction(
            String warehouse,
            String databaseName,
            String tableName,
            String version,
            Boolean isTimestamp,
            Map<String, String> catalogConfig) {
        super(warehouse, databaseName, tableName, catalogConfig);
        this.version = version;
        this.isTimestamp = isTimestamp;
    }

    @Override
    public void run() throws Exception {
        LOG.debug("Run rollback-to action with snapshot id '{}'.", version);

        if (!(table instanceof DataTable)) {
            throw new IllegalArgumentException("Unknown table: " + identifier);
        }

        if (version.chars().allMatch(Character::isDigit)) {
            if (isTimestamp != null && isTimestamp) {
                FileStoreTable fileStoreTable = (FileStoreTable) table;
                fileStoreTable.rollbackTo(
                        fileStoreTable
                                .snapshotManager()
                                .earlierOrEqualTimeMills(Long.parseLong(version))
                                .id());
            } else {
                table.rollbackTo(Long.parseLong(version));
            }
        } else {
            table.rollbackTo(version);
        }
    }
}
