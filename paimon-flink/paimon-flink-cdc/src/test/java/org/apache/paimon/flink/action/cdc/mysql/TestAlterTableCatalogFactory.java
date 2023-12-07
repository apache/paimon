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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.SchemaChange;

import java.util.ArrayList;
import java.util.List;

/**
 * Factory to create a mock catalog for catalog loader test. If catalog loader works, the
 * 'alterTable' method will leave a special option.
 */
public class TestAlterTableCatalogFactory implements CatalogFactory {

    @Override
    public String identifier() {
        return "test-alter-table";
    }

    @Override
    public Catalog create(FileIO fileIO, Path warehouse, CatalogContext context) {
        return new FileSystemCatalog(fileIO, warehouse, context.options()) {

            @Override
            public void alterTable(
                    Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
                    throws TableNotExistException, ColumnAlreadyExistException,
                            ColumnNotExistException {
                List<SchemaChange> newChanges = new ArrayList<>(changes);
                newChanges.add(SchemaChange.setOption("alter-table-test", "true"));
                super.alterTable(identifier, newChanges, ignoreIfNotExists);
            }
        };
    }
}
