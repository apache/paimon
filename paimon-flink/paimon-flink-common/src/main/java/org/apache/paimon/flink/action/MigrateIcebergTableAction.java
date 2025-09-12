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

import org.apache.paimon.flink.procedure.MigrateIcebergTableProcedure;

import java.util.Map;

/** Migrate from iceberg table to paimon table. */
public class MigrateIcebergTableAction extends ActionBase implements LocalAction {

    private final String sourceTableFullName;
    private final String tableProperties;
    private final Integer parallelism;

    private final String icebergProperties;

    public MigrateIcebergTableAction(
            String sourceTableFullName,
            Map<String, String> catalogConfig,
            String icebergProperties,
            String tableProperties,
            Integer parallelism) {
        super(catalogConfig);
        this.sourceTableFullName = sourceTableFullName;
        this.tableProperties = tableProperties;
        this.parallelism = parallelism;
        this.icebergProperties = icebergProperties;
    }

    @Override
    public void executeLocally() throws Exception {
        MigrateIcebergTableProcedure migrateIcebergTableProcedure =
                new MigrateIcebergTableProcedure();
        migrateIcebergTableProcedure.withCatalog(catalog);
        migrateIcebergTableProcedure.call(
                null, sourceTableFullName, icebergProperties, tableProperties, parallelism);
    }
}
