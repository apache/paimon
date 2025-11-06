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

package org.apache.paimon.flink.orphan;

import org.apache.paimon.catalog.Identifier;

import java.io.Serializable;
import java.util.Map;

/** Serializable data structure to hold branch and table information. */
public class BranchTableInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String branch;
    private final String databaseName;
    private final String tableName;
    private final Map<String, String> catalogOptions;

    public BranchTableInfo(
            String branch,
            String databaseName,
            String tableName,
            Map<String, String> catalogOptions) {
        this.branch = branch;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.catalogOptions = catalogOptions;
    }

    public String getBranch() {
        return branch;
    }

    public Identifier getIdentifier() {
        return new Identifier(databaseName, tableName);
    }

    public Map<String, String> getCatalogOptions() {
        return catalogOptions;
    }
}
