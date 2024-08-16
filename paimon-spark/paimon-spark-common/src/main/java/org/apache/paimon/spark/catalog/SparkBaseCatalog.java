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

package org.apache.paimon.spark.catalog;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.spark.SparkProcedures;
import org.apache.paimon.spark.SparkSource;
import org.apache.paimon.spark.analysis.NoSuchProcedureException;
import org.apache.paimon.spark.catalog.functions.PaimonFunctions;
import org.apache.paimon.spark.procedure.Procedure;
import org.apache.paimon.spark.procedure.ProcedureBuilder;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;

import java.util.Arrays;
import java.util.Map;

import scala.Option;

/** Spark base catalog. */
public abstract class SparkBaseCatalog
        implements TableCatalog,
                FunctionCatalog,
                SupportsNamespaces,
                ProcedureCatalog,
                WithPaimonCatalog {

    protected String catalogName;

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public Procedure loadProcedure(Identifier identifier) throws NoSuchProcedureException {
        if (Catalog.SYSTEM_DATABASE_NAME.equals(identifier.namespace()[0])) {
            ProcedureBuilder builder = SparkProcedures.newBuilder(identifier.name());
            if (builder != null) {
                return builder.withTableCatalog(this).build();
            }
        }
        throw new NoSuchProcedureException(identifier);
    }

    public boolean usePaimon(String provider) {
        return provider == null || SparkSource.NAME().equalsIgnoreCase(provider);
    }

    // --------------------- Function Catalog Methods ----------------------------
    private static final Map<String, UnboundFunction> FUNCTIONS =
            ImmutableMap.of("bucket", new PaimonFunctions.BucketFunction());

    @Override
    public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
        UnboundFunction func = FUNCTIONS.get(ident.name());
        if (func == null) {
            throw new NoSuchFunctionException(
                    "Function " + ident + " is not a paimon function", Option.empty());
        }
        return func;
    }

    @Override
    public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length != 0) {
            throw new NoSuchNamespaceException(
                    "Namespace " + Arrays.toString(namespace) + " is not valid", Option.empty());
        }
        return FUNCTIONS.keySet().stream()
                .map(name -> Identifier.of(namespace, name))
                .toArray(Identifier[]::new);
    }
}
