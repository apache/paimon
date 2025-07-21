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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableMetadata;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTUtil;
import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.ThreadUtils;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.CoreOptions.BRANCH;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.TableType.OBJECT_TABLE;
import static org.apache.paimon.catalog.Catalog.COMMENT_PROP;
import static org.apache.paimon.catalog.Catalog.DB_LOCATION_PROP;
import static org.apache.paimon.options.CatalogOptions.FILE_IO_ALLOW_CACHE;
import static org.apache.paimon.rest.RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS;

/** Wrap over RESTCatalog to provide basic operations for virtual path. */
public class VFSOperations {
    private static final Logger LOG = LoggerFactory.getLogger(VFSOperations.class);

    private final RESTApi api;
    private final CatalogContext context;

    // table id -> fileIO
    private static final Cache<RESTToken, FileIO> FILE_IO_CACHE =
            Caffeine.newBuilder()
                    .expireAfterAccess(30, TimeUnit.MINUTES)
                    .maximumSize(1000)
                    .removalListener(
                            (ignored, value, cause) -> IOUtils.closeQuietly((FileIO) value))
                    .scheduler(
                            Scheduler.forScheduledExecutorService(
                                    Executors.newSingleThreadScheduledExecutor(
                                            ThreadUtils.newDaemonThreadFactory(
                                                    "rest-token-file-io-scheduler"))))
                    .build();

    private static final Cache<String, RESTToken> TOKEN_CACHE =
            Caffeine.newBuilder().expireAfterAccess(30, TimeUnit.MINUTES).maximumSize(1000).build();

    public VFSOperations(CatalogContext context) {
        this.context = context;
        this.api = new RESTApi(context.options());
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
        Identifier identifier = new Identifier(databaseName, tableName);
        // Get table from REST server
        TableMetadata table;
        try {
            table = loadTableMetadata(identifier);
        } catch (Catalog.TableNotExistException e) {
            if (parts.length == 2) {
                return new VFSTableRootIdentifier(databaseName, tableName);
            } else {
                return new VFSTableObjectIdentifier(databaseName, tableName);
            }
        }
        if (table.isExternal()) {
            throw new IOException("Do not support visiting external table " + identifier);
        }
        Options options = new Options(table.schema().options());
        // Get real path
        StringBuilder realPath = new StringBuilder(options.get(PATH));
        boolean isTableRoot = true;
        if (parts.length > 2) {
            isTableRoot = false;
            if (!options.get(PATH).endsWith("/")) {
                realPath.append("/");
            }
            for (int i = 2; i < parts.length; i++) {
                realPath.append(parts[i]);
                if (i < parts.length - 1) {
                    realPath.append("/");
                }
            }
        }
        // Get REST token
        FileIO fileIO =
                getFileIO(new Identifier(databaseName, tableName), table, options.get(PATH));

        if (parts.length == 2) {
            return new VFSTableRootIdentifier(
                    table, realPath.toString(), fileIO, databaseName, tableName);
        } else {
            return new VFSTableObjectIdentifier(
                    table, realPath.toString(), fileIO, databaseName, tableName);
        }
    }

    public Database getDatabase(String databaseName) throws FileNotFoundException {
        try {
            GetDatabaseResponse response = api.getDatabase(databaseName);
            Map<String, String> options = new HashMap<>(response.getOptions());
            options.put(DB_LOCATION_PROP, response.getLocation());
            response.putAuditOptionsTo(options);
            return new Database.DatabaseImpl(databaseName, options, options.get(COMMENT_PROP));
        } catch (NoSuchResourceException e) {
            throw new FileNotFoundException("Database " + databaseName + " not found");
        } catch (ForbiddenException e) {
            throw new Catalog.DatabaseNoPermissionException(databaseName, e);
        }
    }

    public List<String> listDatabases() {
        return api.listDatabases();
    }

    public void createDatabase(String databaseName) {
        try {
            api.createDatabase(databaseName, Collections.emptyMap());
        } catch (AlreadyExistsException e) {
            LOG.info("Database {} already exist, no need to create", databaseName);
        } catch (ForbiddenException e) {
            throw new Catalog.DatabaseNoPermissionException(databaseName, e);
        } catch (BadRequestException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public List<String> listTables(String databaseName) throws FileNotFoundException {
        try {
            return api.listTables(databaseName);
        } catch (NoSuchResourceException e) {
            throw new FileNotFoundException("Database " + databaseName + " not found");
        } catch (ForbiddenException e) {
            throw new Catalog.DatabaseNoPermissionException(databaseName, e);
        }
    }

    public void createObjectTable(String databaseName, String tableName)
            throws FileNotFoundException {
        Identifier identifier = Identifier.create(databaseName, tableName);
        Schema schema = Schema.newBuilder().option(TYPE.key(), OBJECT_TABLE.toString()).build();
        try {
            tryCreateObjectTable(identifier, schema);
        } catch (Catalog.DatabaseNotExistException e) {
            // Database not exist, try to create database and then create table again
            createDatabase(databaseName);
            try {
                tryCreateObjectTable(identifier, schema);
            } catch (Catalog.DatabaseNotExistException e1) {
                throw new FileNotFoundException("Database " + databaseName + " not found");
            }
        }
    }

    private void tryCreateObjectTable(Identifier identifier, Schema schema)
            throws Catalog.DatabaseNotExistException {
        try {
            api.createTable(identifier, schema);
        } catch (AlreadyExistsException e) {
            LOG.info("Table {} already exist, no need to create", identifier);
        } catch (NotImplementedException e) {
            throw new RuntimeException(new UnsupportedOperationException(e.getMessage()));
        } catch (NoSuchResourceException e) {
            throw new Catalog.DatabaseNotExistException(identifier.getDatabaseName());
        } catch (BadRequestException e) {
            throw new RuntimeException(new IllegalArgumentException(e.getMessage()));
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private FileIO getFileIO(Identifier identifier, TableMetadata table, String path)
            throws FileNotFoundException {
        RESTToken token = TOKEN_CACHE.getIfPresent(table.uuid());
        if (shouldRefresh(token)) {
            synchronized (TOKEN_CACHE) {
                token = TOKEN_CACHE.getIfPresent(table.uuid());
                if (shouldRefresh(token)) {
                    token = refreshToken(identifier);
                    TOKEN_CACHE.put(table.uuid(), token);
                }
            }
        }

        FileIO fileIO = FILE_IO_CACHE.getIfPresent(token);
        if (fileIO != null) {
            return fileIO;
        }

        synchronized (FILE_IO_CACHE) {
            fileIO = FILE_IO_CACHE.getIfPresent(token);
            if (fileIO != null) {
                return fileIO;
            }

            Options options = context.options();
            // the original options are not overwritten
            options = new Options(RESTUtil.merge(token.token(), options.toMap()));
            options.set(FILE_IO_ALLOW_CACHE, false);
            CatalogContext fileIOContext = CatalogContext.create(options);
            try {
                fileIO = FileIO.get(new Path(path), fileIOContext);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            FILE_IO_CACHE.put(token, fileIO);
            return fileIO;
        }
    }

    private boolean shouldRefresh(RESTToken token) {
        return token == null
                || token.expireAtMillis() - System.currentTimeMillis()
                        < TOKEN_EXPIRATION_SAFE_TIME_MILLIS;
    }

    private RESTToken refreshToken(Identifier identifier) throws FileNotFoundException {
        LOG.info("begin refresh data token for identifier [{}]", identifier);
        GetTableTokenResponse response;
        try {
            response = api.loadTableToken(identifier);
        } catch (NoSuchResourceException e) {
            throw new FileNotFoundException("Table " + identifier + " not found");
        } catch (ForbiddenException e) {
            throw new Catalog.TableNoPermissionException(identifier, e);
        }

        LOG.info(
                "end refresh data token for identifier [{}] expiresAtMillis [{}]",
                identifier,
                response.getExpiresAtMillis());

        RESTToken token = new RESTToken(response.getToken(), response.getExpiresAtMillis());
        return token;
    }

    private TableMetadata loadTableMetadata(Identifier identifier)
            throws Catalog.TableNotExistException {
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
            throw new Catalog.TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new Catalog.TableNoPermissionException(identifier, e);
        }

        return toTableMetadata(identifier.getDatabaseName(), response);
    }

    private TableMetadata toTableMetadata(String db, GetTableResponse response) {
        TableSchema schema = TableSchema.create(response.getSchemaId(), response.getSchema());
        Map<String, String> options = new HashMap<>(schema.options());
        options.put(PATH.key(), response.getPath());
        response.putAuditOptionsTo(options);
        Identifier identifier = Identifier.create(db, response.getName());
        if (identifier.getBranchName() != null) {
            options.put(BRANCH.key(), identifier.getBranchName());
        }
        return new TableMetadata(schema.copy(options), response.isExternal(), response.getId());
    }
}
