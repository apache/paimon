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

package org.apache.paimon.flink.function;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.flink.FlinkFileIOLoader;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import javax.annotation.Nullable;

import java.time.Duration;

/** Converts a serialized blob descriptor to a presigned URL. */
public class DescriptorToPresignedUrlFunction extends ScalarFunction
        implements CatalogAwareFunction {

    @Nullable private String catalogName;
    @Nullable private Options catalogOptions;

    private transient Catalog catalog;
    private transient String sourceTable;
    private transient FileIO fileIO;
    private transient Path tableRoot;

    public DescriptorToPresignedUrlFunction() {}

    DescriptorToPresignedUrlFunction(String catalogName, Catalog catalog) {
        this.catalogName = catalogName;
        this.catalog = catalog;
    }

    @Override
    public void setCatalogContext(
            String catalogName, String defaultDatabase, Options catalogOptions) {
        this.catalogName = catalogName;
        this.catalogOptions = new Options(catalogOptions.toMap());
        this.catalog = null;
        this.sourceTable = null;
        this.fileIO = null;
        this.tableRoot = null;
    }

    @Override
    public void open(FunctionContext context) {
        openCatalog(context.getUserCodeClassLoader());
    }

    @Override
    public void close() throws Exception {
        if (catalog != null) {
            catalog.close();
            catalog = null;
        }
    }

    public String eval(
            String tableName, byte[] descriptorBytes, String extension, Duration validity)
            throws Exception {
        bindSourceTable(tableName);
        if (descriptorBytes == null || extension == null || validity == null) {
            return null;
        }

        loadTable();
        return fileIO.createBlobPresignedUrl(
                tableRoot, BlobDescriptor.deserialize(descriptorBytes), extension, validity);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(
                        InputTypeStrategies.sequence(
                                InputTypeStrategies.and(
                                        InputTypeStrategies.explicit(DataTypes.STRING()),
                                        InputTypeStrategies.LITERAL),
                                InputTypeStrategies.explicit(DataTypes.BYTES()),
                                InputTypeStrategies.explicit(DataTypes.STRING()),
                                InputTypeStrategies.logical(LogicalTypeRoot.INTERVAL_DAY_TIME)))
                .outputTypeStrategy(TypeStrategies.explicit(DataTypes.STRING()))
                .build();
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    private void bindSourceTable(String tableName) {
        if (tableName == null) {
            throw new IllegalArgumentException("source_table must be a non-null literal.");
        }
        if (sourceTable == null) {
            sourceTable = tableName;
        } else if (!sourceTable.equals(tableName)) {
            throw new IllegalArgumentException(
                    "A descriptor URL function instance can use only one source table.");
        }
    }

    private void loadTable() throws Catalog.TableNotExistException {
        if (fileIO != null) {
            return;
        }

        Identifier identifier = toIdentifier(sourceTable);
        Table table = catalog().getTable(identifier);
        if (!(table instanceof DataTable)) {
            throw new IllegalArgumentException(
                    "Source table " + identifier.getFullName() + " is not a data table.");
        }
        DataTable dataTable = (DataTable) table;
        fileIO = dataTable.fileIO();
        tableRoot = dataTable.location();
    }

    private Identifier toIdentifier(String tableName) {
        String[] parts = tableName.split("\\.", -1);
        if (parts.length == 2 && !parts[0].isEmpty() && !parts[1].isEmpty()) {
            return new Identifier(parts[0], parts[1]);
        }
        if (parts.length == 3
                && catalogName != null
                && catalogName.equals(parts[0])
                && !parts[1].isEmpty()
                && !parts[2].isEmpty()) {
            return new Identifier(parts[1], parts[2]);
        }
        throw new IllegalArgumentException(
                "source_table must be 'database.table' or '" + catalogName + ".database.table'.");
    }

    private Catalog catalog() {
        if (catalog == null) {
            openCatalog(Thread.currentThread().getContextClassLoader());
        }
        return catalog;
    }

    private void openCatalog(ClassLoader classLoader) {
        if (catalog != null) {
            return;
        }
        if (catalogOptions == null) {
            throw new IllegalStateException(
                    "DescriptorToPresignedUrlFunction is missing catalog options.");
        }
        catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(catalogOptions, new FlinkFileIOLoader()),
                        classLoader);
    }
}
