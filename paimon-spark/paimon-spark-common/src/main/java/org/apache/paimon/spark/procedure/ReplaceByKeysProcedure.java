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

import org.apache.paimon.spark.commands.ReplaceByKeysCommand$;
import org.apache.paimon.utils.Preconditions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.StringType;

/** Atomically replace existing Data Evolution rows matching a source query's business keys. */
public class ReplaceByKeysProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS = {
        ProcedureParameter.required("table", StringType),
        ProcedureParameter.required("source_query", StringType),
        ProcedureParameter.required("key_columns", DataTypes.createArrayType(StringType, false)),
        ProcedureParameter.optional("target_filter", StringType)
    };

    protected ReplaceByKeysProcedure(TableCatalog catalog) {
        super(catalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return new StructType().add("success", DataTypes.BooleanType, false);
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier ident = toIdentifier(args.getString(0), PARAMETERS[0].name());
        Preconditions.checkArgument(!args.isNullAt(1), "source_query must not be null");
        Preconditions.checkArgument(!args.isNullAt(2), "key_columns must not be null");
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < args.getArray(2).numElements(); i++) {
            Preconditions.checkArgument(
                    !args.getArray(2).isNullAt(i), "Key columns must not be null");
            keys.add(args.getArray(2).getUTF8String(i).toString());
        }
        modifySparkTable(
                ident,
                table -> {
                    ReplaceByKeysCommand$.MODULE$.execute(
                            spark(),
                            createRelation(ident, table),
                            args.getString(1),
                            keys,
                            args.isNullAt(3) ? null : args.getString(3));
                    return null;
                });
        return new InternalRow[] {newInternalRow(true)};
    }

    public static ProcedureBuilder builder() {
        return new Builder<ReplaceByKeysProcedure>() {
            @Override
            public ReplaceByKeysProcedure doBuild() {
                return new ReplaceByKeysProcedure(tableCatalog());
            }
        };
    }
}
