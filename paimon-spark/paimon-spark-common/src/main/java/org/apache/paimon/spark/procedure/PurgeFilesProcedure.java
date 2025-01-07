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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.spark.sql.types.DataTypes.StringType;

/** A procedure to purge files for a table. */
public class PurgeFilesProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {ProcedureParameter.required("table", StringType)};

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", StringType, true, Metadata.empty())
                    });

    private PurgeFilesProcedure(TableCatalog tableCatalog) {
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
                    FileStoreTable fileStoreTable = (FileStoreTable) table;
                    FileIO fileIO = fileStoreTable.fileIO();
                    Path tablePath = fileStoreTable.snapshotManager().tablePath();
                    try {
                        Arrays.stream(fileIO.listStatus(tablePath))
                                .filter(f -> !f.getPath().getName().contains("schema"))
                                .forEach(
                                        fileStatus -> {
                                            try {
                                                fileIO.delete(fileStatus.getPath(), true);
                                            } catch (IOException e) {
                                                throw new RuntimeException(e);
                                            }
                                        });
                        spark().catalog().refreshTable(table.fullName());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    InternalRow outputRow =
                            newInternalRow(
                                    UTF8String.fromString(
                                            String.format(
                                                    "Success purge files with table: %s.",
                                                    fileStoreTable.name())));
                    return new InternalRow[] {outputRow};
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<PurgeFilesProcedure>() {
            @Override
            public PurgeFilesProcedure doBuild() {
                return new PurgeFilesProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "PurgeFilesProcedure";
    }
}
