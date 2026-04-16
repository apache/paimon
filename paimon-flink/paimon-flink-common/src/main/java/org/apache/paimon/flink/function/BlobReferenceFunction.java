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
import org.apache.paimon.data.BlobReference;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Flink scalar function that constructs a serialized {@link BlobReference}.
 *
 * <p>Usage: {@code sys.blob_reference('db.table', 'picture', rowId)}
 */
public class BlobReferenceFunction extends ScalarFunction {

    private final Map<String, String> catalogOptions;
    private transient Catalog catalog;
    private transient ConcurrentHashMap<String, Integer> fieldIdCache;

    public BlobReferenceFunction(Map<String, String> catalogOptions) {
        this.catalogOptions = catalogOptions;
    }

    @Override
    public void open(FunctionContext context) {
        this.catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(catalogOptions)));
        this.fieldIdCache = new ConcurrentHashMap<>();
    }

    @Override
    public void close() throws Exception {
        if (catalog != null) {
            catalog.close();
            catalog = null;
        }
    }

    public byte[] eval(String tableName, String fieldName, long rowId) {
        if (tableName == null || fieldName == null) {
            return null;
        }
        int fieldId = resolveFieldId(tableName, fieldName);
        return new BlobReference(tableName, fieldId, rowId).serialize();
    }

    private int resolveFieldId(String tableName, String fieldName) {
        String key = tableName + "." + fieldName;
        Integer cached = fieldIdCache.get(key);
        if (cached != null) {
            return cached;
        }
        try {
            Table table = catalog.getTable(Identifier.fromString(tableName));
            for (DataField field : table.rowType().getFields()) {
                if (field.name().equals(fieldName)) {
                    fieldIdCache.put(key, field.id());
                    return field.id();
                }
            }
            throw new IllegalArgumentException(
                    "Field '" + fieldName + "' not found in table '" + tableName + "'.");
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }
    }
}
