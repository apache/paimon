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
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataField;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Factory to create a mock case-insensitive catalog for test. */
public class TestCaseInsensitiveCatalogFactory implements CatalogFactory {

    @Override
    public String identifier() {
        return "test-case-insensitive";
    }

    public Catalog createCatalog(CatalogContext context) {
        String warehouse = CatalogFactory.warehouse(context).toUri().toString();

        Path warehousePath = new Path(warehouse);
        FileIO fileIO;
        try {
            fileIO = FileIO.get(warehousePath, context);
            if (fileIO.exists(warehousePath)) {
                checkArgument(
                        fileIO.isDir(warehousePath),
                        "The %s path '%s' should be a directory.",
                        WAREHOUSE.key(),
                        warehouse);
            } else {
                fileIO.mkdirs(warehousePath);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return create(fileIO, warehousePath, context);
    }

    @Override
    public Catalog create(FileIO fileIO, Path warehouse, CatalogContext context) {
        return new FileSystemCatalog(fileIO, warehouse, context.options().toMap()) {
            @Override
            public void createDatabase(String name, boolean ignoreIfExists)
                    throws DatabaseAlreadyExistException {
                checkCaseInsensitive(name);
                super.createDatabase(name, ignoreIfExists);
            }

            @Override
            public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
                    throws TableAlreadyExistException, DatabaseNotExistException {
                checkCaseInsensitive(identifier.getFullName());
                schema.fields().stream().map(DataField::name).forEach(this::checkCaseInsensitive);
                super.createTable(identifier, schema, ignoreIfExists);
            }

            @Override
            public void renameTable(
                    Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
                    throws TableNotExistException, TableAlreadyExistException {
                checkCaseInsensitive(fromTable.getFullName());
                checkCaseInsensitive(toTable.getFullName());
                super.renameTable(fromTable, toTable, ignoreIfNotExists);
            }

            @Override
            public void alterTable(
                    Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
                    throws TableNotExistException, ColumnAlreadyExistException,
                            ColumnNotExistException {
                for (SchemaChange change : changes) {
                    if (change instanceof SchemaChange.AddColumn) {
                        checkCaseInsensitive(((SchemaChange.AddColumn) change).fieldName());
                    } else if (change instanceof SchemaChange.RenameColumn) {
                        checkCaseInsensitive(((SchemaChange.RenameColumn) change).newName());
                    }
                }
                super.alterTable(identifier, changes, ignoreIfNotExists);
            }

            @Override
            public boolean caseSensitive() {
                return false;
            }

            private void checkCaseInsensitive(String s) {
                checkArgument(
                        s.equals(s.toLowerCase()),
                        String.format(
                                "String [%s] cannot contain upper case in case-insensitive catalog.",
                                s));
            }
        };
    }
}
