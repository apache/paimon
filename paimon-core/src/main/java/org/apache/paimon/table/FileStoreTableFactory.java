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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.HybridFileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.StringUtils;

import java.util.Optional;

import static org.apache.paimon.CoreOptions.TABLE_SCHEMA_PATH;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Factory to create {@link FileStoreTable}. */
public class FileStoreTableFactory {

    public static FileStoreTable create(CatalogContext context) {
        FileIO fileIO = new HybridFileIO();
        fileIO.configure(context);
        return create(fileIO, context.options());
    }

    public static FileStoreTable create(FileIO fileIO, Path path) {
        Options options = new Options();
        options.set(TABLE_SCHEMA_PATH, path.toString());
        return create(fileIO, options);
    }

    public static FileStoreTable create(FileIO fileIO, Options options) {
        Path tablePath = CoreOptions.schemaPath(options);
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
            Path tableSchemaPath,
            TableSchema tableSchema,
            Options dynamicOptions,
            CatalogEnvironment catalogEnvironment) {
        CoreOptions coreOptions = CoreOptions.fromMap(tableSchema.options());
        Path tableDataPath = null;
        if (coreOptions.getDataFileExternalPath() != null) {
            String dbAndTableName =
                    tableSchemaPath.getParent().getName() + "/" + tableSchemaPath.getName();
            tableDataPath = new Path(coreOptions.getDataFileExternalPath(), dbAndTableName);
        }
        FileStoreTable table =
                createWithoutFallbackBranch(
                        fileIO,
                        tableSchemaPath,
                        tableSchema,
                        dynamicOptions,
                        catalogEnvironment,
                        tableDataPath);
        Options options = new Options(table.options());
        String fallbackBranch = options.get(CoreOptions.SCAN_FALLBACK_BRANCH);

        if (!StringUtils.isNullOrWhitespaceOnly(fallbackBranch)) {
            Options branchOptions = new Options(dynamicOptions.toMap());
            branchOptions.set(CoreOptions.BRANCH, fallbackBranch);
            Optional<TableSchema> schema =
                    new SchemaManager(fileIO, tableSchemaPath, fallbackBranch).latest();
            checkArgument(
                    schema.isPresent(),
                    "Cannot set '%s' = '%s' because the branch '%s' isn't existed.",
                    CoreOptions.SCAN_FALLBACK_BRANCH.key(),
                    fallbackBranch,
                    fallbackBranch);
            FileStoreTable fallbackTable =
                    createWithoutFallbackBranch(
                            fileIO,
                            tableSchemaPath,
                            schema.get(),
                            branchOptions,
                            catalogEnvironment,
                            tableDataPath);
            table = new FallbackReadFileStoreTable(table, fallbackTable);
        }

        return table;
    }

    public static FileStoreTable createWithoutFallbackBranch(
            FileIO fileIO,
            Path tableSchemaPath,
            TableSchema tableSchema,
            Options dynamicOptions,
            CatalogEnvironment catalogEnvironment,
            Path tableDataPath) {
        FileStoreTable table =
                tableSchema.primaryKeys().isEmpty()
                        ? new AppendOnlyFileStoreTable(
                                fileIO,
                                tableSchemaPath,
                                tableSchema,
                                catalogEnvironment,
                                tableDataPath)
                        : new PrimaryKeyFileStoreTable(
                                fileIO,
                                tableSchemaPath,
                                tableSchema,
                                catalogEnvironment,
                                tableDataPath);
        return table.copy(dynamicOptions.toMap());
    }
}
