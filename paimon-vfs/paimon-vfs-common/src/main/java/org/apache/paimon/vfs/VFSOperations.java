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

package org.apache.paimon.vfs;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.schema.Schema;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Ticker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.TableType.OBJECT_TABLE;
import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;
import static org.apache.paimon.options.CatalogOptions.CACHE_EXPIRE_AFTER_ACCESS;
import static org.apache.paimon.options.CatalogOptions.CACHE_EXPIRE_AFTER_WRITE;

/** Wrap over RESTCatalog to provide basic operations for virtual path. */
public class VFSOperations {

    private static final Logger LOG = LoggerFactory.getLogger(VFSOperations.class);

    private final RESTApi api;
    private final CatalogContext context;

    @Nullable private Cache<Identifier, VFSTableInfo> tableCache;

    public VFSOperations(Options options) {
        this.api = new RESTApi(options);

        if (options.get(CACHE_ENABLED)) {
            Duration expireAfterAccess = options.get(CACHE_EXPIRE_AFTER_ACCESS);
            if (expireAfterAccess.isZero() || expireAfterAccess.isNegative()) {
                throw new IllegalArgumentException(
                        "When 'cache.expire-after-access' is set to negative or 0, the catalog cache should be disabled.");
            }
            Duration expireAfterWrite = options.get(CACHE_EXPIRE_AFTER_WRITE);
            if (expireAfterWrite.isZero() || expireAfterWrite.isNegative()) {
                throw new IllegalArgumentException(
                        "When 'cache.expire-after-write' is set to negative or 0, the catalog cache should be disabled.");
            }
            LOG.info(
                    "Initialize virtual file system with table cache enabled, expireAfterAccess={}, expireAfterWrite={}",
                    expireAfterAccess,
                    expireAfterWrite);

            tableCache =
                    Caffeine.newBuilder()
                            .softValues()
                            .executor(Runnable::run)
                            .expireAfterAccess(expireAfterAccess)
                            .expireAfterWrite(expireAfterWrite)
                            .ticker(Ticker.systemTicker())
                            .build();
        } else {
            LOG.info("Initialize virtual file system with table cache disabled");
        }
        // Get the configured options which has been merged from REST Server
        this.context = CatalogContext.create(api.options());
    }

    public VFSIdentifier getVFSIdentifier(String virtualPath) throws IOException {
        if (virtualPath.startsWith("/")) {
            virtualPath = virtualPath.substring(1);
        }
        String[] parts = virtualPath.split("/");
        if (virtualPath.isEmpty() || parts.length == 0) {
            return new VFSCatalogIdentifier();
        } else if (parts.length == 1) {
            return new VFSDatabaseIdentifier(parts[0]);
        }
        // parts.length >= 2: table or table object
        String databaseName = parts[0];
        String tableName = parts[1];
        String relativePath = null;
        if (parts.length > 2) {
            relativePath = String.join("/", Arrays.copyOfRange(parts, 2, parts.length));
        }
        Identifier identifier = new Identifier(databaseName, tableName);
        try {
            VFSTableInfo tableInfo = getTableInfo(identifier);
            return relativePath == null
                    ? new VFSTableRootIdentifier(databaseName, tableName, tableInfo)
                    : new VFSTableObjectIdentifier(
                            databaseName, tableName, relativePath, tableInfo);
        } catch (FileNotFoundException e) {
            return relativePath == null
                    ? new VFSTableRootIdentifier(databaseName, tableName)
                    : new VFSTableObjectIdentifier(databaseName, tableName, relativePath);
        }
    }

    public GetDatabaseResponse getDatabase(String databaseName) throws IOException {
        try {
            return api.getDatabase(databaseName);
        } catch (NoSuchResourceException e) {
            throw new FileNotFoundException("Database " + databaseName + " not found");
        } catch (ForbiddenException e) {
            throw new IOException("No permission to access database " + databaseName);
        }
    }

    public List<String> listDatabases() {
        return api.listDatabases();
    }

    public void createDatabase(String databaseName) throws IOException {
        try {
            api.createDatabase(databaseName, Collections.emptyMap());
        } catch (AlreadyExistsException e) {
            LOG.info("Database {} already exist, no need to create", databaseName);
        } catch (ForbiddenException e) {
            throw new IOException("No permission to create database " + databaseName);
        } catch (BadRequestException e) {
            throw new IOException("Bad request when creating database " + databaseName, e);
        }
    }

    public void dropDatabase(String databaseName, boolean recursive) throws IOException {
        try {
            if (!recursive && !api.listTables(databaseName).isEmpty()) {
                throw new IOException(
                        "Database "
                                + databaseName
                                + " is not empty, set recursive to true to drop it");
            }
            api.dropDatabase(databaseName);
            // Remove table cache
            if (isCacheEnabled()) {
                List<Identifier> tables = new ArrayList<>();
                for (Identifier identifier : tableCache.asMap().keySet()) {
                    if (identifier.getDatabaseName().equals(databaseName)) {
                        tables.add(identifier);
                    }
                }
                tables.forEach(tableCache::invalidate);
            }
        } catch (NoSuchResourceException e) {
            throw new FileNotFoundException("Database " + databaseName + " not found");
        } catch (ForbiddenException e) {
            throw new IOException("No permission to drop database " + databaseName);
        }
    }

    public List<String> listTables(String databaseName) throws IOException {
        try {
            return api.listTables(databaseName);
        } catch (NoSuchResourceException e) {
            throw new FileNotFoundException("Database " + databaseName + " not found");
        } catch (ForbiddenException e) {
            throw new IOException("No permission to access database " + databaseName);
        }
    }

    public void createObjectTable(String databaseName, String tableName) throws IOException {
        Identifier identifier = Identifier.create(databaseName, tableName);
        Schema schema = Schema.newBuilder().option(TYPE.key(), OBJECT_TABLE.toString()).build();
        try {
            tryCreateObjectTable(identifier, schema);
        } catch (FileNotFoundException e) {
            // Database not exist, try to create database and then create table again
            createDatabase(databaseName);
            tryCreateObjectTable(identifier, schema);
        }
    }

    public void dropTable(String databaseName, String tableName) throws IOException {
        Identifier identifier = Identifier.create(databaseName, tableName);
        try {
            api.dropTable(identifier);
            // Remove table cache
            if (isCacheEnabled()) {
                tableCache.invalidate(identifier);
            }
        } catch (NoSuchResourceException e) {
            throw new FileNotFoundException("Table " + identifier + " not found");
        } catch (ForbiddenException e) {
            throw new IOException("No permission to drop table " + identifier);
        }
    }

    public void renameTable(String databaseName, String srcTableName, String dstTableName)
            throws IOException {
        Identifier srcIdentifier = Identifier.create(databaseName, srcTableName);
        Identifier dstIdentifier = Identifier.create(databaseName, dstTableName);
        try {
            api.renameTable(srcIdentifier, dstIdentifier);
            // Remove table cache
            if (isCacheEnabled()) {
                tableCache.invalidate(srcIdentifier);
                tableCache.invalidate(dstIdentifier);
            }
        } catch (NoSuchResourceException e) {
            throw new FileNotFoundException("Source table " + srcIdentifier + " not found");
        } catch (ForbiddenException e) {
            throw new IOException(
                    "No permission to rename table " + srcIdentifier + " to " + dstIdentifier);
        } catch (AlreadyExistsException e) {
            throw new FileAlreadyExistsException(
                    "Target table " + dstIdentifier + " already exist");
        } catch (BadRequestException e) {
            throw new IOException(
                    "Bad request when renaming table " + srcIdentifier + " to " + dstIdentifier, e);
        }
    }

    private void tryCreateObjectTable(Identifier identifier, Schema schema) throws IOException {
        try {
            api.createTable(identifier, schema);
        } catch (AlreadyExistsException e) {
            LOG.info("Table {} already exist, no need to create", identifier);
        } catch (NotImplementedException e) {
            throw new IOException("Create object table not implemented");
        } catch (NoSuchResourceException e) {
            throw new FileNotFoundException("Database not found");
        } catch (BadRequestException e) {
            throw new IOException("Bad request when creating table " + identifier, e);
        } catch (IllegalArgumentException e) {
            throw new IOException("Illegal argument when creating table " + identifier, e);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private GetTableResponse loadTableMetadata(Identifier identifier) throws IOException {
        // if the table is system table, we need to load table metadata from the system table's data
        // table
        Identifier loadTableIdentifier =
                identifier.isSystemTable()
                        ? new Identifier(
                                identifier.getDatabaseName(),
                                identifier.getTableName(),
                                identifier.getBranchName())
                        : identifier;

        GetTableResponse response;
        try {
            response = api.getTable(loadTableIdentifier);
        } catch (NoSuchResourceException e) {
            throw new FileNotFoundException("Table not found");
        } catch (ForbiddenException e) {
            throw new IOException("No permission to access table " + identifier);
        }

        return response;
    }

    private VFSTableInfo loadTableInfo(Identifier identifier) throws IOException {
        // Get table from REST server
        GetTableResponse table;
        table = loadTableMetadata(identifier);

        if (table.isExternal()) {
            throw new IOException("Do not support visiting external table " + identifier);
        }
        Path tablePath = new Path(table.getPath());
        FileIO fileIO = new RESTTokenFileIO(context, api, identifier, tablePath);
        return new VFSTableInfo(table.getId(), tablePath, fileIO);
    }

    private VFSTableInfo getTableInfo(Identifier identifier) throws IOException {
        if (!isCacheEnabled()) {
            return loadTableInfo(identifier);
        }
        VFSTableInfo vfsTableInfo = tableCache.getIfPresent(identifier);
        if (vfsTableInfo != null) {
            return vfsTableInfo;
        }
        vfsTableInfo = loadTableInfo(identifier);
        tableCache.put(identifier, vfsTableInfo);
        return vfsTableInfo;
    }

    @VisibleForTesting
    public boolean isCacheEnabled() {
        return tableCache != null;
    }
}
