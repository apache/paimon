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

package org.apache.paimon.spark;

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import org.apache.spark.SparkEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

/** Utils for Spark. */
public class SparkUtils {

    public static IOManager createIOManager() {
        String[] localDirs = SparkEnv.get().blockManager().diskBlockManager().localDirsString();
        return new IOManagerImpl(localDirs);
    }

    /** This mimics a class inside of Spark which is private inside of LookupCatalog. */
    public static class CatalogAndIdentifier {
        private final CatalogPlugin catalog;
        private final Identifier identifier;

        public CatalogAndIdentifier(Pair<CatalogPlugin, Identifier> identifier) {
            this.catalog = identifier.getLeft();
            this.identifier = identifier.getRight();
        }

        public CatalogPlugin catalog() {
            return catalog;
        }

        public Identifier identifier() {
            return identifier;
        }
    }

    /**
     * A modified version of Spark's LookupCatalog.CatalogAndIdentifier.unapply Attempts to find the
     * catalog and identifier a multipart identifier represents.
     *
     * @param nameParts Multipart identifier representing a table
     * @return The CatalogPlugin and Identifier for the table
     */
    public static <C, T> Pair<C, T> catalogAndIdentifier(
            List<String> nameParts,
            Function<String, C> catalogProvider,
            BiFunction<String[], String, T> identifierProvider,
            C currentCatalog,
            String[] currentNamespace) {
        Preconditions.checkArgument(
                !nameParts.isEmpty(), "Cannot determine catalog and identifier from empty name");

        int lastElementIndex = nameParts.size() - 1;
        String name = nameParts.get(lastElementIndex);

        if (nameParts.size() == 1) {
            // Only a single element, use current catalog and namespace
            return Pair.of(currentCatalog, identifierProvider.apply(currentNamespace, name));
        } else {
            C catalog = catalogProvider.apply(nameParts.get(0));
            if (catalog == null) {
                // The first element was not a valid catalog, treat it like part of the namespace
                String[] namespace = nameParts.subList(0, lastElementIndex).toArray(new String[0]);
                return Pair.of(currentCatalog, identifierProvider.apply(namespace, name));
            } else {
                // Assume the first element is a valid catalog
                String[] namespace = nameParts.subList(1, lastElementIndex).toArray(new String[0]);
                return Pair.of(catalog, identifierProvider.apply(namespace, name));
            }
        }
    }

    /**
     * A modified version of Spark's LookupCatalog.CatalogAndIdentifier.unapply Attempts to find the
     * catalog and identifier a multipart identifier represents.
     *
     * @param spark Spark session to use for resolution
     * @param nameParts Multipart identifier representing a table
     * @param defaultCatalog Catalog to use if none is specified
     * @return The CatalogPlugin and Identifier for the table
     */
    public static CatalogAndIdentifier catalogAndIdentifier(
            SparkSession spark, List<String> nameParts, CatalogPlugin defaultCatalog) {
        CatalogManager catalogManager = spark.sessionState().catalogManager();

        String[] currentNamespace;
        if (defaultCatalog.equals(catalogManager.currentCatalog())) {
            currentNamespace = catalogManager.currentNamespace();
        } else {
            currentNamespace = defaultCatalog.defaultNamespace();
        }

        Pair<CatalogPlugin, Identifier> catalogIdentifier =
                SparkUtils.catalogAndIdentifier(
                        nameParts,
                        catalogName -> {
                            try {
                                return catalogManager.catalog(catalogName);
                            } catch (Exception e) {
                                return null;
                            }
                        },
                        Identifier::of,
                        defaultCatalog,
                        currentNamespace);
        return new CatalogAndIdentifier(catalogIdentifier);
    }

    public static CatalogAndIdentifier catalogAndIdentifier(
            SparkSession spark, String name, CatalogPlugin defaultCatalog) throws ParseException {
        ParserInterface parser = spark.sessionState().sqlParser();
        Seq<String> multiPartIdentifier = parser.parseMultipartIdentifier(name).toIndexedSeq();
        List<String> javaMultiPartIdentifier = JavaConverters.seqAsJavaList(multiPartIdentifier);
        return catalogAndIdentifier(spark, javaMultiPartIdentifier, defaultCatalog);
    }

    public static CatalogAndIdentifier catalogAndIdentifier(
            String description, SparkSession spark, String name, CatalogPlugin defaultCatalog) {
        try {
            return catalogAndIdentifier(spark, name, defaultCatalog);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Cannot parse " + description + ": " + name, e);
        }
    }
}
