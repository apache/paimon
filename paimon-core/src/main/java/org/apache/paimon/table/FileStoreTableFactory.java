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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.PATH;

/** Factory to create {@link FileStoreTable}. */
public class FileStoreTableFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreTableFactory.class);

    public static FileStoreTable create(CatalogContext context) {
        FileIO fileIO;
        try {
            fileIO = FileIO.get(CoreOptions.path(context.options()), context);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return create(fileIO, context.options());
    }

    public static FileStoreTable create(FileIO fileIO, Path path) {
        Options options = new Options();
        options.set(PATH, path.toString());
        return create(fileIO, options);
    }

    public static FileStoreTable create(FileIO fileIO, Options options) {
        Path tablePath = CoreOptions.path(options);
        String branchName = CoreOptions.branch(options.toMap());
        TableSchema tableSchema =
                new SchemaManager(fileIO, tablePath, branchName)
                        .latest()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Schema file not found in location "
                                                        + tablePath
                                                        + ". Please create table first."));

        return create(fileIO, tablePath, tableSchema, options, CatalogEnvironment.empty());
    }

    public static FileStoreTable create(FileIO fileIO, Path tablePath, TableSchema tableSchema) {
        return create(fileIO, tablePath, tableSchema, new Options(), CatalogEnvironment.empty());
    }

    public static FileStoreTable create(
            FileIO fileIO,
            Path tablePath,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        return create(fileIO, tablePath, tableSchema, new Options(), catalogEnvironment);
    }

    public static FileStoreTable create(
            FileIO fileIO,
            Path tablePath,
            TableSchema tableSchema,
            Options dynamicOptions,
            CatalogEnvironment catalogEnvironment) {
        FileStoreTable table =
                createWithoutFallbackBranch(
                        fileIO, tablePath, tableSchema, dynamicOptions, catalogEnvironment);

        Options options = new Options(table.options());
        String fallbackBranch = options.get(CoreOptions.SCAN_FALLBACK_BRANCH);
        if (ChainTableUtils.isChainTable(options.toMap())) {
            table = createChainTable(table, fileIO, tablePath, dynamicOptions, catalogEnvironment);
        } else if (!StringUtils.isNullOrWhitespaceOnly(fallbackBranch)) {
            Options branchOptions = new Options(dynamicOptions.toMap());
            branchOptions.set(CoreOptions.BRANCH, fallbackBranch);
            Optional<TableSchema> schema =
                    new SchemaManager(fileIO, tablePath, fallbackBranch).latest();
            if (schema.isPresent()) {
                Identifier identifier = catalogEnvironment.identifier();
                CatalogEnvironment fallbackCatalogEnvironment = catalogEnvironment;
                if (identifier != null) {
                    fallbackCatalogEnvironment =
                            catalogEnvironment.copy(
                                    new Identifier(
                                            identifier.getDatabaseName(),
                                            identifier.getObjectName(),
                                            fallbackBranch));
                }
                FileStoreTable fallbackTable =
                        createWithoutFallbackBranch(
                                fileIO,
                                tablePath,
                                schema.get(),
                                branchOptions,
                                fallbackCatalogEnvironment);
                table = new FallbackReadFileStoreTable(table, fallbackTable);
            } else {
                LOG.error("Fallback branch {} not found for table {}", fallbackBranch, tablePath);
            }
        }

        return table;
    }

    public static FileStoreTable createChainTable(
            FileStoreTable table,
            FileIO fileIO,
            Path tablePath,
            Options dynamicOptions,
            CatalogEnvironment catalogEnvironment) {
        String scanFallbackSnapshotBranch =
                table.options().get(CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key());
        String scanFallbackDeltaBranch =
                table.options().get(CoreOptions.SCAN_FALLBACK_DELTA_BRANCH.key());
        String currentBranch = table.schema().options().get(CoreOptions.BRANCH.key());
        if (scanFallbackSnapshotBranch == null || scanFallbackDeltaBranch == null) {
            return table;
        }

        boolean scanSnapshotBranch = scanFallbackSnapshotBranch.equalsIgnoreCase(currentBranch);
        boolean scanDeltaBranch = scanFallbackDeltaBranch.equalsIgnoreCase(currentBranch);
        LOG.info(
                "Create chain table, tbl path {}, snapshotBranch {}, deltaBranch{}, currentBranch {} "
                        + "scanSnapshotBranch{} scanDeltaBranch {}.",
                tablePath,
                scanFallbackSnapshotBranch,
                scanFallbackDeltaBranch,
                currentBranch,
                scanSnapshotBranch,
                scanDeltaBranch);
        if (scanSnapshotBranch || scanDeltaBranch) {
            return table;
        }

        Options snapshotBranchOptions = new Options(dynamicOptions.toMap());
        snapshotBranchOptions.set(CoreOptions.BRANCH, scanFallbackSnapshotBranch);
        Optional<TableSchema> snapshotSchema =
                new SchemaManager(fileIO, tablePath, scanFallbackSnapshotBranch).latest();
        AbstractFileStoreTable snapshotTable =
                (AbstractFileStoreTable)
                        createWithoutFallbackBranch(
                                fileIO,
                                tablePath,
                                snapshotSchema.get(),
                                snapshotBranchOptions,
                                catalogEnvironment);
        Options deltaBranchOptions = new Options(dynamicOptions.toMap());
        deltaBranchOptions.set(CoreOptions.BRANCH, scanFallbackDeltaBranch);
        Optional<TableSchema> deltaSchema =
                new SchemaManager(fileIO, tablePath, scanFallbackDeltaBranch).latest();
        AbstractFileStoreTable deltaTable =
                (AbstractFileStoreTable)
                        createWithoutFallbackBranch(
                                fileIO,
                                tablePath,
                                deltaSchema.get(),
                                deltaBranchOptions,
                                catalogEnvironment);
        FileStoreTable chainGroupFileStoreTable =
                new ChainGroupReadTable(snapshotTable, deltaTable);
        return new FallbackReadFileStoreTable(table, chainGroupFileStoreTable);
    }

    public static FileStoreTable createWithoutFallbackBranch(
            FileIO fileIO,
            Path tablePath,
            TableSchema tableSchema,
            Options dynamicOptions,
            CatalogEnvironment catalogEnvironment) {
        FileStoreTable table =
                tableSchema.primaryKeys().isEmpty()
                        ? new AppendOnlyFileStoreTable(
                                fileIO, tablePath, tableSchema, catalogEnvironment)
                        : new PrimaryKeyFileStoreTable(
                                fileIO, tablePath, tableSchema, catalogEnvironment);
        return table.copy(dynamicOptions.toMap());
    }
}
