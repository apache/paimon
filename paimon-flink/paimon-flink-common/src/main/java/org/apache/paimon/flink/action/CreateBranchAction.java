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

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/** Create branch action for Flink. */
public class CreateBranchAction extends TableActionBase {
    private final String branchName;
    private final String tagName;

    private final Long snapshotId;

    public CreateBranchAction(
            String warehouse,
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String branchName,
            String tagName,
            Long snapshotId) {
        super(warehouse, databaseName, tableName, catalogConfig);
        this.branchName = branchName;
        this.tagName = tagName;
        this.snapshotId = snapshotId;
    }

    @Override
    public void run() throws Exception {
        if (!StringUtils.isBlank(tagName)) {
            table.createBranch(branchName, tagName);
        } else if (snapshotId != null) {
            table.createBranch(branchName, snapshotId);
        } else {
            table.createBranch(branchName);
        }
    }
}
