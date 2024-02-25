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

package org.apache.paimon.branch;

/** {@link TableBranch} has branch relevant information for table. */
public class TableBranch {
    private final String branchName;
    private final String createdFromTag;
    private final Long createdFromSnapshot;

    private final long createTime;

    public TableBranch(
            String branchName, String createdFromTag, Long createdFromSnapshot, long createTime) {
        this.branchName = branchName;
        this.createdFromTag = createdFromTag;
        this.createdFromSnapshot = createdFromSnapshot;
        this.createTime = createTime;
    }

    public String getBranchName() {
        return branchName;
    }

    public String getCreatedFromTag() {
        return createdFromTag;
    }

    public Long getCreatedFromSnapshot() {
        return createdFromSnapshot;
    }

    public long getCreateTime() {
        return createTime;
    }
}
