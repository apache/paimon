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

import org.apache.paimon.operation.RemoveUnexistingManifests;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Procedure to remove unexisting manifest files from the manifest list. See {@link
 * RemoveUnexistingManifests} for detailed use cases.
 *
 * <pre><code>
 *  -- remove unexisting manifest files in table `mydb.myt`
 *  CALL sys.remove_unexisting_manifests(table => 'mydb.myt')
 *
 *  -- remove unexisting manifest files in a branch
 *  CALL sys.remove_unexisting_manifests(table => 'mydb.`myt$branch_rt`')
 * </code></pre>
 *
 * <p>Note that the user is on their own risk using this procedure, which may cause data loss when
 * used outside of the documented repair cases.
 */
public class RemoveUnexistingManifestsProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {ProcedureParameter.required("table", DataTypes.StringType)};

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.StringType, false, Metadata.empty())
                    });

    private RemoveUnexistingManifestsProcedure(TableCatalog tableCatalog) {
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
        return modifyPaimonTable(
                tableIdent,
                table -> {
                    Preconditions.checkArgument(
                            table instanceof FileStoreTable,
                            "%s is not a file store table",
                            tableIdent);
                    new RemoveUnexistingManifests((FileStoreTable) table).execute();
                    return new InternalRow[] {newInternalRow(UTF8String.fromString("Success"))};
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<RemoveUnexistingManifestsProcedure>() {
            @Override
            public RemoveUnexistingManifestsProcedure doBuild() {
                return new RemoveUnexistingManifestsProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "RemoveUnexistingManifestsProcedure";
    }
}
