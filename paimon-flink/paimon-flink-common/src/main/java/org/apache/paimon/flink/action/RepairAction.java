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

import org.apache.paimon.flink.procedure.RepairProcedure;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.table.procedure.DefaultProcedureContext;

import java.util.Map;

/** Repair action for Flink. */
public class RepairAction extends ActionBase {

    private String tableName;

    private String databaseName;

    protected RepairAction(
            String warehouse,
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig) {
        super(warehouse, catalogConfig);
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public void run() throws Exception {
        Preconditions.checkArgument(!tableName.contains("\\."), "tableName can't contain ','");

        RepairProcedure repairProcedure = new RepairProcedure();
        repairProcedure.withCatalog(catalog);
        String identifier = databaseName.concat(".").concat(tableName);
        repairProcedure.call(new DefaultProcedureContext(env), identifier);
    }
}
