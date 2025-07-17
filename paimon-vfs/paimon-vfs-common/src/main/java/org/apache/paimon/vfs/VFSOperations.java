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
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTUtil;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.ThreadUtils;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.TableType.OBJECT_TABLE;
import static org.apache.paimon.options.CatalogOptions.FILE_IO_ALLOW_CACHE;
import static org.apache.paimon.rest.RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS;

/** Wrap over RESTCatalog to provide basic operations for virtual path. */
public class VFSOperations {
    private static final Logger LOG = LoggerFactory.getLogger(VFSOperations.class);

    private RESTCatalog catalog;

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
        catalog = new RESTCatalog(context);
    }

    public VFSIdentifier getVFSIdentifier(String virtualPath) throws TableNotFoundException {
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
            table = catalog.loadTableMetadata(identifier);
        } catch (Catalog.TableNotExistException e) {
            if (parts.length == 2) {
                return new VFSTableRootIdentifier(databaseName, tableName);
            } else {
                return new VFSTableObjectIdentifier(databaseName, tableName);
            }
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

    public Database getDatabase(String databaseName) throws TableNotFoundException {
        try {
            return catalog.getDatabase(databaseName);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new TableNotFoundException("Database " + databaseName + " not found", e);
        }
    }

    public List<String> listDatabases() {
        return catalog.listDatabases();
    }

    public void createDatabase(String databaseName) {
        try {
            catalog.createDatabase(databaseName, true);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            LOG.info("Database {} already exist, no need to create", databaseName);
        }
    }

    public List<String> listTables(String databaseName) throws TableNotFoundException {
        try {
            return catalog.listTables(databaseName);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new TableNotFoundException("Database " + databaseName + " not found", e);
        }
    }

    public void createObjectTable(String databaseName, String tableName)
            throws TableNotFoundException {
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
                throw new TableNotFoundException("Database " + databaseName + " not found", e1);
            }
        }
    }

    private void tryCreateObjectTable(Identifier identifier, Schema schema)
            throws Catalog.DatabaseNotExistException {
        try {
            catalog.createTable(identifier, schema, true);
        } catch (Catalog.DatabaseNotExistException e) {
            throw e;
        } catch (Catalog.TableAlreadyExistException e) {
            LOG.info("Table {} already exist, no need to create", identifier);
        }
    }

    private FileIO getFileIO(Identifier identifier, TableMetadata table, String path)
            throws TableNotFoundException {
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

            CatalogContext context = catalog.catalogLoader().context();
            Options options = context.options();
            // the original options are not overwritten
            options = new Options(RESTUtil.merge(token.token(), options.toMap()));
            options.set(FILE_IO_ALLOW_CACHE, false);
            context = CatalogContext.create(options, context.preferIO(), context.fallbackIO());
            try {
                fileIO = FileIO.get(new Path(path), context);
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

    private RESTToken refreshToken(Identifier identifier) throws TableNotFoundException {
        LOG.info("begin refresh data token for identifier [{}]", identifier);
        GetTableTokenResponse response;
        try {
            response = catalog.loadTableToken(identifier);
        } catch (Catalog.TableNotExistException e) {
            throw new TableNotFoundException("Table not found", e);
        }

        LOG.info(
                "end refresh data token for identifier [{}] expiresAtMillis [{}]",
                identifier,
                response.getExpiresAtMillis());

        RESTToken token = new RESTToken(response.getToken(), response.getExpiresAtMillis());
        return token;
    }
}
