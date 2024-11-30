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

package org.apache.paimon.privilege;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.object.ObjectTable;

import java.util.Map;

/** A {@link PrivilegedFileStoreTable} for {@link ObjectTable}. */
public class PrivilegedObjectTable extends PrivilegedFileStoreTable implements ObjectTable {

    private final ObjectTable objectTable;

    protected PrivilegedObjectTable(
            ObjectTable wrapped, PrivilegeChecker privilegeChecker, Identifier identifier) {
        super(wrapped, privilegeChecker, identifier);
        this.objectTable = wrapped;
    }

    @Override
    public String objectLocation() {
        return objectTable.objectLocation();
    }

    @Override
    public FileStoreTable underlyingTable() {
        return objectTable.underlyingTable();
    }

    @Override
    public FileIO objectFileIO() {
        return objectTable.objectFileIO();
    }

    @Override
    public long refresh() {
        privilegeChecker.assertCanInsert(identifier);
        return objectTable.refresh();
    }

    // ======================= copy ============================

    @Override
    public PrivilegedObjectTable copy(Map<String, String> dynamicOptions) {
        return new PrivilegedObjectTable(
                objectTable.copy(dynamicOptions), privilegeChecker, identifier);
    }

    @Override
    public PrivilegedObjectTable copy(TableSchema newTableSchema) {
        return new PrivilegedObjectTable(
                objectTable.copy(newTableSchema), privilegeChecker, identifier);
    }

    @Override
    public PrivilegedObjectTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new PrivilegedObjectTable(
                objectTable.copyWithoutTimeTravel(dynamicOptions), privilegeChecker, identifier);
    }

    @Override
    public PrivilegedObjectTable copyWithLatestSchema() {
        return new PrivilegedObjectTable(
                objectTable.copyWithLatestSchema(), privilegeChecker, identifier);
    }

    @Override
    public PrivilegedObjectTable switchToBranch(String branchName) {
        return new PrivilegedObjectTable(
                objectTable.switchToBranch(branchName), privilegeChecker, identifier);
    }
}
