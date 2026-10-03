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
import org.apache.paimon.operation.RemoveUnexistingManifests;
import org.apache.paimon.table.FileStoreTable;

import java.util.Map;

/** Action to remove the un-existing manifest file. */
public class RemoveUnexistingManifestsAction extends ActionBase implements LocalAction {

    private final String databaseName;
    private final String tableName;

    public RemoveUnexistingManifestsAction(
            String databaseName, String tableName, Map<String, String> catalogConfig) {
        super(catalogConfig);
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public void executeLocally() throws Exception {
        Identifier identifier = new Identifier(databaseName, tableName);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        new RemoveUnexistingManifests(table).execute();
    }
}
