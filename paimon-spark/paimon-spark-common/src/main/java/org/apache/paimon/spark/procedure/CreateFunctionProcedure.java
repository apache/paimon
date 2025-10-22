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
import org.apache.paimon.function.FunctionImpl;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.spark.utils.CatalogUtils;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.ParameterUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * create function procedure. Usage:
 *
 * <pre><code>
 *  -- NOTE: use '' as placeholder for optional arguments
 *
 *  CALL sys.create_function('function_identifier',
 *     '[{"id": 0, "name":"length", "type":"INT"}', '{"id": 1, "name":"width", "type":"INT"}]',
 *     '[{"id": 0, "name":"area", "type":"BIGINT"]',
 *     true, 'comment', 'k1=v1,k2=v2'
 *    )
 *
 * </code></pre>
 */
public class CreateFunctionProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("function", StringType),
                ProcedureParameter.required("inputParams", StringType),
                ProcedureParameter.required("returnParams", StringType),
                ProcedureParameter.optional("deterministic", BooleanType),
                ProcedureParameter.optional("comment", StringType),
                ProcedureParameter.optional("options", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected CreateFunctionProcedure(TableCatalog tableCatalog) {
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
        String paimonCatalogName = ((WithPaimonCatalog) tableCatalog()).paimonCatalogName();
        org.apache.spark.sql.connector.catalog.Identifier ident =
                toIdentifier(args.getString(0), PARAMETERS[0].name());
        Identifier function = CatalogUtils.toIdentifier(ident, paimonCatalogName);
        List<DataField> inputParams = getDataFieldsFromArguments(1, args);
        List<DataField> returnParams = getDataFieldsFromArguments(2, args);
        boolean deterministic = args.isNullAt(3) ? true : args.getBoolean(3);
        String comment = args.isNullAt(4) ? null : args.getString(4);
        String properties = args.isNullAt(5) ? null : args.getString(5);
        Map<String, String> options = ParameterUtils.parseCommaSeparatedKeyValues(properties);
        try {
            FunctionImpl functionImpl =
                    new FunctionImpl(
                            function,
                            inputParams,
                            returnParams,
                            deterministic,
                            Maps.newHashMap(),
                            comment,
                            options);
            paimonCatalog.createFunction(function, functionImpl, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new InternalRow[] {newInternalRow(true)};
    }

    public static ProcedureBuilder builder() {
        return new Builder<CreateFunctionProcedure>() {
            @Override
            public CreateFunctionProcedure doBuild() {
                return new CreateFunctionProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "CreateFunctionProcedure";
    }

    public static List<DataField> getDataFieldsFromArguments(int position, InternalRow args) {
        String data = args.isNullAt(position) ? null : args.getString(position);
        return ParameterUtils.parseDataFieldArray(data);
    }
}
