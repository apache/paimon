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

import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.table.FileStoreTable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Compact manifest procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.compact_manifest(table => 'tableId')
 * </code></pre>
 */
public class CompactManifestProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {ProcedureParameter.required("table", StringType)};

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected CompactManifestProcedure(TableCatalog tableCatalog) {
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
        FileStoreTable table = (FileStoreTable) loadSparkTable(tableIdent).getTable();

        try (FileStoreCommit commit =
                table.store()
                        .newCommit(table.coreOptions().createCommitUser())
                        .ignoreEmptyCommit(false)) {
            commit.compactManifest();
        }

        return new InternalRow[] {newInternalRow(true)};
    }

    @Override
    public String description() {
        return "This procedure execute compact action on paimon table.";
    }

    public static ProcedureBuilder builder() {
        return new Builder<CompactManifestProcedure>() {
            @Override
            public CompactManifestProcedure doBuild() {
                return new CompactManifestProcedure(tableCatalog());
            }
        };
    }
}
