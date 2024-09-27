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

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.tag.TagTimeExpire;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.time.LocalDateTime;
import java.util.List;
import java.util.TimeZone;

import static org.apache.spark.sql.types.DataTypes.StringType;

/** A procedure to expire tags by time. */
public class ExpireTagsProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("older_than", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("expired_tags", StringType, false, Metadata.empty())
                    });

    protected ExpireTagsProcedure(TableCatalog tableCatalog) {
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
        String olderThanStr = args.isNullAt(1) ? null : args.getString(1);
        return modifyPaimonTable(
                tableIdent,
                table -> {
                    FileStoreTable fileStoreTable = (FileStoreTable) table;
                    TagTimeExpire tagTimeExpire =
                            fileStoreTable.store().newTagCreationManager().getTagTimeExpire();
                    if (olderThanStr != null) {
                        LocalDateTime olderThanTime =
                                DateTimeUtils.parseTimestampData(
                                                olderThanStr, 3, TimeZone.getDefault())
                                        .toLocalDateTime();
                        tagTimeExpire.withOlderThanTime(olderThanTime);
                    }
                    List<String> expired = tagTimeExpire.expire();
                    return expired.isEmpty()
                            ? new InternalRow[] {
                                newInternalRow(UTF8String.fromString("No expired tags."))
                            }
                            : expired.stream()
                                    .map(x -> newInternalRow(UTF8String.fromString(x)))
                                    .toArray(InternalRow[]::new);
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<ExpireTagsProcedure>() {
            @Override
            public ExpireTagsProcedure doBuild() {
                return new ExpireTagsProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ExpireTagsProcedure";
    }
}
