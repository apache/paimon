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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Repair Procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.repair([database_or_table => 'tableId'])
 * </code></pre>
 */
public class RepairProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {ProcedureParameter.required("database_or_table", StringType)};

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected RepairProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Catalog paimonCatalog = ((WithPaimonCatalog) tableCatalog()).paimonCatalog();
        String identifier = args.getString(0);
        try {
            if (StringUtils.isBlank(identifier)) {
                paimonCatalog.repairCatalog();
                return new InternalRow[] {newInternalRow(true)};
            }

            String[] paths = identifier.split("\\.");
            switch (paths.length) {
                case 1:
                    paimonCatalog.repairDatabase(paths[0]);
                    break;
                case 2:
                    paimonCatalog.repairTable(Identifier.create(paths[0], paths[1]));
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Cannot get splits from '%s' to get database and table",
                                    identifier));
            }

        } catch (Exception e) {
            throw new RuntimeException("Call repair error", e);
        }
        return new InternalRow[] {newInternalRow(true)};
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<RepairProcedure>() {
            @Override
            public RepairProcedure doBuild() {
                return new RepairProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "RepairProcedure";
    }
}
