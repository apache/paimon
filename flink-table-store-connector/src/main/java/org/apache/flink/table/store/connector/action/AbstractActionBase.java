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

package org.apache.flink.table.store.connector.action;

import org.apache.flink.table.store.file.catalog.Catalog;
import org.apache.flink.table.store.file.catalog.CatalogFactory;
import org.apache.flink.table.store.file.catalog.Identifier;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.options.CatalogOptions;
import org.apache.flink.table.store.options.Options;
import org.apache.flink.table.store.table.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Abstract base of {@link Action}. */
public abstract class AbstractActionBase implements Action {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractActionBase.class);

    protected final Catalog catalog;

    protected final Identifier identifier;

    protected Table table;

    AbstractActionBase(Path tablePath) {
        this(tablePath, new Options());
    }

    AbstractActionBase(Path tablePath, Options options) {
        String warehouse = tablePath.getParent().getParent().getPath();
        identifier = Identifier.fromPath(tablePath);

        catalog =
                CatalogFactory.createCatalog(
                        CatalogOptions.create(
                                new Options().set(CatalogOptions.WAREHOUSE, warehouse)));

        try {
            table = catalog.getTable(identifier);
            if (options.toMap().size() > 0) {
                table = table.copy(options.toMap());
            }
        } catch (Catalog.TableNotExistException e) {
            LOG.error(String.format("Table doesn't exist in path %s.", tablePath.getPath()), e);
            System.err.printf("Table doesn't exist in path %s.\n", tablePath.getPath());
            System.exit(1);
        }
    }
}
