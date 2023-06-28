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
import org.apache.paimon.WriteMode;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.CommitCallback;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.CoreOptions.PATH;

/** Factory to create {@link FileStoreTable}. */
public class FileStoreTableFactory {

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
        TableSchema tableSchema =
                new SchemaManager(fileIO, tablePath)
                        .latest()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Schema file not found in location "
                                                        + tablePath
                                                        + ". Please create table first."));
        return create(
                fileIO,
                tablePath,
                tableSchema,
                options,
                Lock.emptyFactory(),
                Collections.emptyList());
    }

    public static FileStoreTable create(FileIO fileIO, Path tablePath, TableSchema tableSchema) {
        return create(
                fileIO,
                tablePath,
                tableSchema,
                new Options(),
                Lock.emptyFactory(),
                Collections.emptyList());
    }

    public static FileStoreTable create(
            FileIO fileIO,
            Path tablePath,
            TableSchema tableSchema,
            Lock.Factory lockFactory,
            List<CommitCallback.Factory> commitCallbackFactories) {
        return create(
                fileIO,
                tablePath,
                tableSchema,
                new Options(),
                lockFactory,
                commitCallbackFactories);
    }

    public static FileStoreTable create(
            FileIO fileIO,
            Path tablePath,
            TableSchema tableSchema,
            Options dynamicOptions,
            Lock.Factory lockFactory,
            List<CommitCallback.Factory> commitCallbackFactories) {
        FileStoreTable table;
        Options coreOptions = Options.fromMap(tableSchema.options());
        WriteMode writeMode = coreOptions.get(CoreOptions.WRITE_MODE);
        if (writeMode == WriteMode.AUTO) {
            writeMode =
                    tableSchema.primaryKeys().isEmpty()
                            ? WriteMode.APPEND_ONLY
                            : WriteMode.CHANGE_LOG;
            coreOptions.set(CoreOptions.WRITE_MODE, writeMode);
        }
        if (writeMode == WriteMode.APPEND_ONLY) {
            table =
                    new AppendOnlyFileStoreTable(
                            fileIO, tablePath, tableSchema, lockFactory, commitCallbackFactories);
        } else {
            if (tableSchema.primaryKeys().isEmpty()) {
                table =
                        new ChangelogValueCountFileStoreTable(
                                fileIO,
                                tablePath,
                                tableSchema,
                                lockFactory,
                                commitCallbackFactories);
            } else {
                table =
                        new ChangelogWithKeyFileStoreTable(
                                fileIO,
                                tablePath,
                                tableSchema,
                                lockFactory,
                                commitCallbackFactories);
            }
        }
        return table.copy(dynamicOptions.toMap());
    }
}
