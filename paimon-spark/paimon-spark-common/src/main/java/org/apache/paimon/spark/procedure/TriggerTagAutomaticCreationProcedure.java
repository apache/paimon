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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.tag.TagPeriodHandler;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.time.LocalDateTime;
import java.util.Collections;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** A procedure to trigger the tag automatic creation for a table. */
public class TriggerTagAutomaticCreationProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("force", BooleanType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    private TriggerTagAutomaticCreationProcedure(TableCatalog tableCatalog) {
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
        boolean force = !args.isNullAt(1) && args.getBoolean(1);
        return modifyPaimonTable(
                tableIdent,
                table -> {
                    FileStoreTable fsTable = ((FileStoreTable) table);
                    String commitUser = CoreOptions.fromMap(table.options()).createCommitUser();

                    try (TableCommitImpl commit = fsTable.newCommit(commitUser)) {
                        TagAutoManager tam = fsTable.newTagAutoManager();
                        if (tam.getTagAutoCreation() != null) {
                            // Fist try
                            tam.run();
                            // If the expected tag not created, try again with an empty commit
                            if (force
                                    && !isAutoTagExits(fsTable)
                                    && tam.getTagAutoCreation().forceCreatingSnapshot()) {
                                ManifestCommittable committable =
                                        new ManifestCommittable(
                                                Long.MAX_VALUE, null, Collections.emptyList());
                                commit.ignoreEmptyCommit(false);
                                commit.commit(committable);
                                // Second try
                                tam.run();
                            }
                        }

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    return new InternalRow[] {newInternalRow(true)};
                });
    }

    private boolean isAutoTagExits(FileStoreTable table) {
        TagPeriodHandler periodHandler =
                TagPeriodHandler.create(CoreOptions.fromMap(table.options()));

        // With forceCreatingSnapshot, the auto-tag should exist for "now"
        LocalDateTime tagTime = periodHandler.normalizeToPreviousTag(LocalDateTime.now());
        String tagName = periodHandler.timeToTag(tagTime);
        return table.tagManager().tagExists(tagName);
    }

    public static ProcedureBuilder builder() {
        return new Builder<TriggerTagAutomaticCreationProcedure>() {
            @Override
            public TriggerTagAutomaticCreationProcedure doBuild() {
                return new TriggerTagAutomaticCreationProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "TriggerTagAutomaticCreationProcedure";
    }
}
