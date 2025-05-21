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

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;

/** Catalog methods for working with Functions. */
public interface SupportFunction extends FunctionCatalog, SupportsNamespaces {

    default boolean isFunctionNamespace(String[] namespace) {
        // Allow for empty namespace, as Spark's bucket join will use `bucket` function with empty
        // namespace to generate transforms for partitioning.
        // Otherwise, check if it is paimon namespace.
        return namespace.length == 0 || (namespace.length == 1 && namespaceExists(namespace));
    }

    @Override
    default Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
        if (isFunctionNamespace(namespace)) {
            return PaimonFunctions.names().stream()
                    .map(name -> Identifier.of(namespace, name))
                    .toArray(Identifier[]::new);
        }

        throw new NoSuchNamespaceException(namespace);
    }
}
