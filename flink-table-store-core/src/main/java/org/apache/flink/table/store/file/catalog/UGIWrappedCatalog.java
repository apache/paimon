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

package org.apache.flink.table.store.file.catalog;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.store.file.schema.SchemaChange;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.utils.RunnableWithException;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/** A {@link Catalog} wrapped hadoop {@link UserGroupInformation}. */
public class UGIWrappedCatalog implements Catalog {

    private final Catalog catalog;

    public UGIWrappedCatalog(Supplier<Catalog> supplier) {
        this.catalog = doAsUgiCatchEx(supplier::get);
    }

    @Override
    public Optional<CatalogLock.Factory> lockFactory() {
        return doAsUgiCatchEx(catalog::lockFactory);
    }

    @Override
    public List<String> listDatabases() {
        return doAsUgiCatchEx(catalog::listDatabases);
    }

    @Override
    public boolean databaseExists(String databaseName) {
        return doAsUgiCatchEx(() -> catalog.databaseExists(databaseName));
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        try {
            doAsUgi(() -> catalog.createDatabase(name, ignoreIfExists));
        } catch (Exception e) {
            if (e instanceof DatabaseAlreadyExistException) {
                throw (DatabaseAlreadyExistException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        try {
            doAsUgi(() -> catalog.dropDatabase(name, ignoreIfNotExists, cascade));
        } catch (Exception e) {
            if (e instanceof DatabaseNotExistException) {
                throw (DatabaseNotExistException) e;
            }
            if (e instanceof DatabaseNotEmptyException) {
                throw (DatabaseNotEmptyException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        try {
            return doAsUgi(() -> catalog.listTables(databaseName));
        } catch (Exception e) {
            if (e instanceof DatabaseNotExistException) {
                throw (DatabaseNotExistException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public Path getTableLocation(ObjectPath tablePath) {
        return doAsUgiCatchEx(() -> catalog.getTableLocation(tablePath));
    }

    @Override
    public TableSchema getTableSchema(ObjectPath tablePath) throws TableNotExistException {
        try {
            return doAsUgi(() -> catalog.getTableSchema(tablePath));
        } catch (Exception e) {
            if (e instanceof TableNotExistException) {
                throw (TableNotExistException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public Table getTable(ObjectPath tablePath) throws TableNotExistException {
        try {
            return doAsUgi(() -> catalog.getTable(tablePath));
        } catch (Exception e) {
            if (e instanceof TableNotExistException) {
                throw (TableNotExistException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) {
        return doAsUgiCatchEx(() -> catalog.tableExists(tablePath));
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException {
        try {
            doAsUgi(() -> catalog.dropTable(tablePath, ignoreIfNotExists));
        } catch (Exception e) {
            if (e instanceof TableNotExistException) {
                throw (TableNotExistException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, UpdateSchema tableSchema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        try {
            doAsUgi(() -> catalog.createTable(tablePath, tableSchema, ignoreIfExists));
        } catch (Exception e) {
            if (e instanceof TableAlreadyExistException) {
                throw (TableAlreadyExistException) e;
            }
            if (e instanceof DatabaseNotExistException) {
                throw (DatabaseNotExistException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException {
        try {
            doAsUgi(() -> catalog.alterTable(tablePath, changes, ignoreIfNotExists));
        } catch (Exception e) {
            if (e instanceof TableNotExistException) {
                throw (TableNotExistException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        doAsUgi(catalog::close);
    }

    private static void doAsUgi(RunnableWithException action) throws Exception {
        doAsUgi(
                (PrivilegedExceptionAction<Void>)
                        () -> {
                            action.run();
                            return null;
                        });
    }

    private static <T> T doAsUgiCatchEx(PrivilegedExceptionAction<T> action) {
        try {
            return doAsUgi(action);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> T doAsUgi(PrivilegedExceptionAction<T> action) throws Exception {
        try {
            return UserGroupInformation.getLoginUser().doAs(action);
        } catch (UndeclaredThrowableException undeclared) {
            Throwable throwable = undeclared.getUndeclaredThrowable();
            if (throwable instanceof Exception) {
                throw (Exception) throwable;
            } else {
                throw new RuntimeException(throwable);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
