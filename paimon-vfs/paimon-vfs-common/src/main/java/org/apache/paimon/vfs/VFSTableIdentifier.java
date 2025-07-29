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

package org.apache.paimon.vfs;

import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

/** Identifier for table. */
public abstract class VFSTableIdentifier implements VFSIdentifier {

    protected final String databaseName;
    protected final String tableName;

    protected final @Nullable VFSTableInfo tableInfo;

    // Constructor for non-exist table
    public VFSTableIdentifier(String databaseName, String tableName) {
        this(databaseName, tableName, null);
    }

    // Constructor for existing table
    public VFSTableIdentifier(
            String databaseName, String tableName, @Nullable VFSTableInfo tableInfo) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableInfo = tableInfo;
    }

    public String databaseName() {
        return databaseName;
    }

    public String tableName() {
        return tableName;
    }

    @Nullable
    public VFSTableInfo tableInfo() {
        return tableInfo;
    }

    public abstract Path filePath();
}
