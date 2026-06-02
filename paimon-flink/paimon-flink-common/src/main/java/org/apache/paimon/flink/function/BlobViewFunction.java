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
import org.apache.paimon.data.BlobViewStruct;
import org.apache.paimon.flink.FlinkFileIOLoader;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/** Flink scalar function that constructs a serialized {@link BlobViewStruct}. */
public class BlobViewFunction extends ScalarFunction implements CatalogAwareFunction {

    @Nullable private String catalogName;
    @Nullable private Options catalogOptions;

    private transient Catalog catalog;
    private transient Map<String, Map<String, BlobViewField>> blobViewFieldCache;

    @Override
    public void setCatalogContext(
            String catalogName, String defaultDatabase, Options catalogOptions) {
        this.catalogName = catalogName;
        this.catalogOptions = new Options(catalogOptions.toMap());
        this.catalog = null;
        this.blobViewFieldCache = null;
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

    public byte[] eval(String identifier, int fieldId, long rowId) {
        if (identifier == null) {
            return null;
        }
        return new BlobViewStruct(Identifier.fromString(identifier), fieldId, rowId).serialize();
    }

    public byte[] eval(String tableName, String fieldName, long rowId) {
        if (tableName == null || fieldName == null) {
            return null;
        }

        BlobViewField field = blobViewField(tableName, fieldName);
        return new BlobViewStruct(field.tableName, field.fieldId, rowId).serialize();
    }

    private BlobViewField blobViewField(String tableName, String fieldName) {
        Map<String, BlobViewField> tableCache = blobViewFieldCache().get(tableName);
        if (tableCache != null) {
            BlobViewField cached = tableCache.get(fieldName);
            if (cached != null) {
                return cached;
            }
        }

        Identifier identifier = toIdentifier(tableName);
        BlobViewField field = new BlobViewField(identifier, fieldId(identifier, fieldName));
        if (tableCache == null) {
            tableCache = new HashMap<>();
            blobViewFieldCache().put(tableName, tableCache);
        }
        tableCache.put(fieldName, field);
        return field;
    }

    private int fieldId(Identifier identifier, String fieldName) {
        try {
            Table table = catalog().getTable(identifier);
            if (!table.rowType().containsField(fieldName)) {
                throw new IllegalArgumentException(
                        "Cannot find blob field "
                                + fieldName
                                + " in upstream table "
                                + identifier.getFullName()
                                + ".");
            }
            DataField field = table.rowType().getField(fieldName);
            if (!field.type().is(DataTypeRoot.BLOB)) {
                throw new IllegalArgumentException(
                        "Field "
                                + fieldName
                                + " in upstream table "
                                + identifier.getFullName()
                                + " is not a BLOB field.");
            }
            return field.id();
        } catch (Catalog.TableNotExistException e) {
            throw new IllegalArgumentException(
                    "Cannot find upstream table " + identifier.getFullName() + ".", e);
        }
    }

    private Identifier toIdentifier(String tableName) {
        String[] parts = tableName.split("\\.", 3);
        if (parts.length == 2) {
            return new Identifier(parts[0], parts[1]);
        }
        if (catalogName != null && catalogName.equals(parts[0])) {
            return new Identifier(parts[1], parts[2]);
        }
        throw new IllegalArgumentException(
                "Table name must be 'database.table' or '"
                        + catalogName
                        + ".database.table', but is '"
                        + tableName
                        + "'.");
    }

    private Catalog catalog() {
        if (catalog == null) {
            openCatalog(Thread.currentThread().getContextClassLoader());
        }
        return catalog;
    }

    private Map<String, Map<String, BlobViewField>> blobViewFieldCache() {
        if (blobViewFieldCache == null) {
            blobViewFieldCache = new HashMap<>();
        }
        return blobViewFieldCache;
    }

    private void openCatalog(ClassLoader classLoader) {
        if (catalog != null) {
            return;
        }
        if (catalogOptions == null) {
            throw new IllegalStateException("BlobViewFunction is missing catalog options.");
        }
        catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(catalogOptions, new FlinkFileIOLoader()),
                        classLoader);
    }

    private static class BlobViewField {
        private final Identifier tableName;
        private final int fieldId;

        private BlobViewField(Identifier tableName, int fieldId) {
            this.tableName = tableName;
            this.fieldId = fieldId;
        }
    }
}
