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
import org.apache.paimon.spark.SparkTypeUtils;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewImpl;

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.spark.utils.CatalogUtils.checkNamespace;
import static org.apache.paimon.spark.utils.CatalogUtils.toIdentifier;

/** Catalog methods for working with Views. */
public interface SupportView extends WithPaimonCatalog {

    default List<String> listViews(String[] namespace) throws NoSuchNamespaceException {
        try {
            checkNamespace(namespace);
            return paimonCatalog().listViews(namespace[0]);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new NoSuchNamespaceException(namespace);
        }
    }

    default View loadView(Identifier ident) throws Catalog.ViewNotExistException {
        return paimonCatalog().getView(toIdentifier(ident));
    }

    default void createView(
            Identifier ident,
            StructType schema,
            String queryText,
            String comment,
            Map<String, String> properties,
            Boolean ignoreIfExists)
            throws NoSuchNamespaceException {
        org.apache.paimon.catalog.Identifier paimonIdent = toIdentifier(ident);
        try {
            paimonCatalog()
                    .createView(
                            paimonIdent,
                            new ViewImpl(
                                    paimonIdent,
                                    SparkTypeUtils.toPaimonRowType(schema),
                                    queryText,
                                    comment,
                                    properties),
                            ignoreIfExists);
        } catch (Catalog.ViewAlreadyExistException e) {
            throw new RuntimeException("view already exists: " + ident, e);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new NoSuchNamespaceException(ident.namespace());
        }
    }

    default void dropView(Identifier ident, Boolean ignoreIfExists) {
        try {
            paimonCatalog().dropView(toIdentifier(ident), ignoreIfExists);
        } catch (Catalog.ViewNotExistException e) {
            throw new RuntimeException("view not exists: " + ident, e);
        }
    }
}
