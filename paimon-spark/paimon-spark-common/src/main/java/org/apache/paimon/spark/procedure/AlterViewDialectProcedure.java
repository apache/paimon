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
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.view.DialectChange;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * alter view dialect procedure. Usage:
 *
 * <pre><code>
 *  -- NOTE: use '' as placeholder for optional arguments
 *
 *  -- add dialect in the view
 *  CALL sys.alert_view_dialect('viewId', 'add', 'dialect', 'query')
 *
 *  -- update dialect in the view
 *  CALL sys.alert_view_dialect('viewId', 'update', 'dialect', 'query')
 *
 *  -- drop dialect in the view
 *  CALL sys.alert_view_dialect('viewId', 'drop', 'dialect')
 *
 * </code></pre>
 */
public class AlterViewDialectProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("view", StringType),
                ProcedureParameter.required("action", StringType),
                ProcedureParameter.required("dialect", StringType),
                ProcedureParameter.optional("query", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected AlterViewDialectProcedure(TableCatalog tableCatalog) {
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
        Catalog paimonCatalog = ((WithPaimonCatalog) tableCatalog()).paimonCatalog();
        Identifier view = Identifier.fromString(args.getString(0));
        DialectChange dialectChange;
        switch (args.getString(1)) {
            case "add":
                {
                    dialectChange = DialectChange.add(args.getString(2), args.getString(3));
                    break;
                }
            case "update":
                {
                    dialectChange = DialectChange.update(args.getString(2), args.getString(3));
                    break;
                }
            case "drop":
                {
                    dialectChange = DialectChange.drop(args.getString(2));
                    break;
                }
            default:
                {
                    throw new IllegalArgumentException("Unsupported action: " + args.getString(1));
                }
        }
        try {
            paimonCatalog.alterView(view, dialectChange, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new InternalRow[] {newInternalRow(true)};
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<AlterViewDialectProcedure>() {
            @Override
            public AlterViewDialectProcedure doBuild() {
                return new AlterViewDialectProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "AlterViewDialectProcedure";
    }
}
