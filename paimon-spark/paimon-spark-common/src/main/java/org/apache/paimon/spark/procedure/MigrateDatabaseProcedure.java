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
import org.apache.paimon.migrate.Migrator;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.spark.utils.TableMigrationUtils;
import org.apache.paimon.utils.ParameterUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Migrate database procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.migrate_database(source_type => 'hive', database => 'db01', options => 'x1=y1,x2=y2')
 * </code></pre>
 */
public class MigrateDatabaseProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("source_type", StringType),
                ProcedureParameter.required("database", StringType),
                ProcedureParameter.optional("options", StringType),
                ProcedureParameter.optional(
                        "options_map", DataTypes.createMapType(StringType, StringType)),
                ProcedureParameter.optional("parallelism", IntegerType),
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected MigrateDatabaseProcedure(TableCatalog tableCatalog) {
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
        String database = args.getString(1);
        String properties = args.isNullAt(2) ? null : args.getString(2);
        MapData mapData = args.isNullAt(3) ? null : args.getMap(3);
        Map<String, String> optionMap = mapDataToHashMap(mapData);
        int parallelism =
                args.isNullAt(4) ? Runtime.getRuntime().availableProcessors() : args.getInt(4);

        Catalog paimonCatalog = ((WithPaimonCatalog) tableCatalog()).paimonCatalog();

        Map<String, String> options = ParameterUtils.parseCommaSeparatedKeyValues(properties);
        options.putAll(optionMap);

        try {
            List<Migrator> migrators =
                    TableMigrationUtils.getImporters(
                            format, paimonCatalog, database, parallelism, options);

            for (Migrator migrator : migrators) {
                migrator.executeMigrate();
                migrator.renameTable(false);
            }
        } catch (Exception e) {
            throw new RuntimeException("Call migrate_database error: " + e.getMessage(), e);
        }

        return new InternalRow[] {newInternalRow(true)};
    }

    public static Map<String, String> mapDataToHashMap(MapData mapData) {
        HashMap<String, String> map = new HashMap<>();
        if (mapData != null) {
            for (int index = 0; index < mapData.numElements(); index++) {
                map.put(
                        mapData.keyArray().getUTF8String(index).toString(),
                        mapData.valueArray().getUTF8String(index).toString());
            }
        }
        return map;
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<MigrateDatabaseProcedure>() {
            @Override
            public MigrateDatabaseProcedure doBuild() {
                return new MigrateDatabaseProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "MigrateDatabaseProcedure";
    }
}
