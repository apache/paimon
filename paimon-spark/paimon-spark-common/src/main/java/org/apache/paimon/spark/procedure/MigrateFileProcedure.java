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
import org.apache.paimon.migrate.Migrator;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.spark.utils.TableMigrationUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Migrate file procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.migrate_file(source_type => 'hive', source_table => 'db.source_tbl', target_table => 'db.target_tbl')
 * </code></pre>
 */
public class MigrateFileProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("source_type", StringType),
                ProcedureParameter.required("source_table", StringType),
                ProcedureParameter.required("target_table", StringType),
                ProcedureParameter.optional("delete_origin", BooleanType),
                ProcedureParameter.optional("parallelism", IntegerType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", BooleanType, true, Metadata.empty())
                    });

    protected MigrateFileProcedure(TableCatalog tableCatalog) {
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
        String format = args.getString(0);
        String sourceTable = args.getString(1);
        String targetTable = args.getString(2);
        boolean deleteNeed = args.isNullAt(3) ? true : args.getBoolean(3);
        int parallelism =
                args.isNullAt(4) ? Runtime.getRuntime().availableProcessors() : args.getInt(6);

        Identifier sourceTableId = Identifier.fromString(sourceTable);
        Identifier targetTableId = Identifier.fromString(targetTable);

        Catalog paimonCatalog = ((WithPaimonCatalog) tableCatalog()).paimonCatalog();

        if (!(paimonCatalog.tableExists(targetTableId))) {
            throw new IllegalArgumentException(
                    "Target paimon table does not exist: " + targetTable);
        }

        try {
            Migrator migrator =
                    TableMigrationUtils.getImporter(
                            format,
                            paimonCatalog,
                            sourceTableId.getDatabaseName(),
                            sourceTableId.getObjectName(),
                            targetTableId.getDatabaseName(),
                            targetTableId.getObjectName(),
                            parallelism,
                            Collections.emptyMap());

            migrator.deleteOriginTable(deleteNeed);
            migrator.executeMigrate();
        } catch (Exception e) {
            throw new RuntimeException("Call migrate_file error", e);
        }

        return new InternalRow[] {newInternalRow(true)};
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<MigrateFileProcedure>() {
            @Override
            public MigrateFileProcedure doBuild() {
                return new MigrateFileProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "MigrateFileProcedure";
    }
}
