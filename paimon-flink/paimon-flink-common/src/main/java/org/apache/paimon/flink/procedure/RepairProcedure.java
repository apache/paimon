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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Repair procedure. Usage:
 *
 * <pre><code>
 *  -- repair all databases and tables in catalog
 *  CALL sys.repair()
 *
 *  -- repair all tables in a specific database
 *  CALL sys.repair('databaseName')
 *
 *  -- repair a table
 *  CALL sys.repair('databaseName.tableName')
 * </code></pre>
 */
public class RepairProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "repair";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    public String[] call(ProcedureContext procedureContext)
            throws Catalog.TableNotExistException, Catalog.DatabaseNotExistException {
        return call(procedureContext, null);
    }

    public String[] call(ProcedureContext procedureContext, String identifier)
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        if (!(catalog instanceof HiveCatalog)) {
            throw new IllegalArgumentException("Only support Hive Catalog");
        }
        HiveCatalog hiveCatalog = (HiveCatalog) catalog;

        if (StringUtils.isBlank(identifier)) {
            hiveCatalog.repairCatalog();
            return new String[] {"Success"};
        }
        String[] paths = identifier.split("\\.");
        switch (paths.length) {
            case 1:
                hiveCatalog.repairDatabase(paths[0]);
                break;
            case 2:
                hiveCatalog.repairTable(Identifier.create(paths[0], paths[1]));
                break;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot get splits from '%s' to get database and table",
                                identifier));
        }

        return new String[] {"Success"};
    }
}
