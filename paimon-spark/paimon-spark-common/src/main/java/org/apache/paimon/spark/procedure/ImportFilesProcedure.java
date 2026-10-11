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

import org.apache.paimon.migrate.ExternalFileImporter;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Imports files from a directory into a partition using external paths. */
public class ImportFilesProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS = {
        ProcedureParameter.required("table", StringType),
        ProcedureParameter.required("location", StringType),
        ProcedureParameter.optional("partition", StringType)
    };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("imported_files", LongType, false, Metadata.empty())
                    });

    private ImportFilesProcedure(TableCatalog tableCatalog) {
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
        Identifier ident = toIdentifier(args.getString(0), PARAMETERS[0].name());
        return modifyPaimonTable(
                ident,
                table -> {
                    try {
                        long importedFiles =
                                ExternalFileImporter.importFiles(
                                        table,
                                        args.getString(1),
                                        args.isNullAt(2) ? null : args.getString(2));
                        return new InternalRow[] {newInternalRow(importedFiles)};
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to import files", e);
                    }
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<ImportFilesProcedure>() {
            @Override
            public ImportFilesProcedure doBuild() {
                return new ImportFilesProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ImportFilesProcedure";
    }
}
