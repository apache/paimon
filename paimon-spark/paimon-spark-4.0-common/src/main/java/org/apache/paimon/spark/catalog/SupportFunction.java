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

import org.apache.paimon.spark.catalog.functions.PaimonFunctions;

import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;

import java.util.Arrays;
import java.util.HashMap;

import scala.Option;
import scala.collection.immutable.Map;

import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;

/** Catalog methods for working with Functions. */
public interface SupportFunction extends FunctionCatalog, SupportsNamespaces {

    static boolean isFunctionNamespace(String[] namespace) {
        // Allow for empty namespace, as Spark's bucket join will use `bucket` function with empty
        // namespace to generate transforms for partitioning. Otherwise, use `sys` namespace.
        return namespace.length == 0 || isSystemNamespace(namespace);
    }

    static boolean isSystemNamespace(String[] namespace) {
        return namespace.length == 1 && namespace[0].equalsIgnoreCase(SYSTEM_DATABASE_NAME);
    }

    @Override
    default Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
        if (isFunctionNamespace(namespace)) {
            return PaimonFunctions.names().stream()
                    .map(name -> Identifier.of(namespace, name))
                    .toArray(Identifier[]::new);
        } else if (namespaceExists(namespace)) {
            return new Identifier[0];
        }

        throw new NoSuchNamespaceException(
                "Namespace " + Arrays.toString(namespace) + " is not valid", (Map<String, String>) new HashMap<String,String>());
    }

    @Override
    default UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
        if (isFunctionNamespace(ident.namespace())) {
            UnboundFunction func = PaimonFunctions.load(ident.name());
            if (func != null) {
                return func;
            }
        }

        throw new NoSuchFunctionException(
                "Function " + ident + " is not a paimon function", (Map<String, String>) new HashMap<String,String>());
    }
}
