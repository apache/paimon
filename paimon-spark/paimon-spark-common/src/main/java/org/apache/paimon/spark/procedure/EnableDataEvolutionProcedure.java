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

import org.apache.paimon.append.dataevolution.DataEvolutionEnabler;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.spark.utils.CatalogUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Enables data evolution on an existing append table without rewriting its data files: every data
 * file gets a first row id and {@code row-tracking.enabled} / {@code data-evolution.enabled} are
 * switched on in a new schema. See {@link DataEvolutionEnabler}.
 *
 * <pre><code>
 *  CALL sys.enable_data_evolution(table => 'default.T')
 *  CALL sys.enable_data_evolution(table => 'default.T', dry_run => true)
 * </code></pre>
 */
public class EnableDataEvolutionProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("dry_run", BooleanType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", StringType, true, Metadata.empty())
                    });

    protected EnableDataEvolutionProcedure(TableCatalog tableCatalog) {
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
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        boolean dryRun = !args.isNullAt(1) && args.getBoolean(1);

        Catalog paimonCatalog = ((WithPaimonCatalog) tableCatalog()).paimonCatalog();
        String paimonCatalogName = ((WithPaimonCatalog) tableCatalog()).paimonCatalogName();
        org.apache.paimon.catalog.Identifier identifier =
                CatalogUtils.toIdentifier(tableIdent, paimonCatalogName);

        DataEvolutionEnabler.Result result;
        try {
            result = new DataEvolutionEnabler(paimonCatalog, identifier).run(dryRun);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (!dryRun && !result.skipped) {
            // the table changed schema: drop the cached Spark plan
            refreshSparkCache(tableIdent, loadSparkTable(tableIdent));
        }
        return new InternalRow[] {
            newInternalRow(UTF8String.fromString(result.describe(identifier)))
        };
    }

    public static ProcedureBuilder builder() {
        return new Builder<EnableDataEvolutionProcedure>() {
            @Override
            public EnableDataEvolutionProcedure doBuild() {
                return new EnableDataEvolutionProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "EnableDataEvolutionProcedure";
    }
}
