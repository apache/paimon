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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeCasts;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Base class for update data fields process function. */
public abstract class UpdatedDataFieldsProcessFunctionBase<I, O> extends ProcessFunction<I, O> {
    private static final Logger LOG =
            LoggerFactory.getLogger(UpdatedDataFieldsProcessFunctionBase.class);

    protected Catalog catalog;
    protected final Catalog.Loader catalogLoader;

    protected UpdatedDataFieldsProcessFunctionBase(Catalog.Loader catalogLoader) {
        this.catalogLoader = catalogLoader;
    }

    @Override
    public void open(Configuration parameters) {
        this.catalog = catalogLoader.load();
    }

    protected void commitSchemaChange(Identifier identifier, Schema tableChange)
            throws Catalog.ColumnAlreadyExistException, Catalog.TableNotExistException,
                    Catalog.ColumnNotExistException {
        List<SchemaChange> schemaChanges = extractSchemaChanges(identifier, tableChange);
        catalog.alterTable(identifier, schemaChanges, false);
    }

    public static boolean canConvert(DataType oldType, DataType newType) {
        return DataTypeCasts.supportsExplicitCast(oldType, newType)
                && CastExecutors.resolve(oldType, newType) != null;
    }

    protected List<SchemaChange> extractSchemaChanges(Identifier identifier, Schema tableChange)
            throws Catalog.TableNotExistException {
        Table table = catalog.getTable(identifier);
        Map<String, DataField> oldFields =
                table.rowType().getFields().stream()
                        .collect(
                                Collectors.toMap(
                                        DataField::name, Function.identity(), (v1, v2) -> v2));

        // field change
        List<SchemaChange> result = new ArrayList<>();
        for (DataField newField : tableChange.fields()) {
            if (oldFields.containsKey(newField.name())) {
                DataField oldField = oldFields.get(newField.name());
                // we compare by ignoring nullable, because partition keys and primary keys might be
                // nullable in source database, but they can't be null in Paimon
                if (oldField.type().equalsIgnoreNullable(newField.type())) {
                    if (!Objects.equals(oldField.description(), newField.description())) {
                        result.add(
                                SchemaChange.updateColumnComment(
                                        new String[] {newField.name()}, newField.description()));
                    }
                } else {
                    result.add(SchemaChange.updateColumnType(newField.name(), newField.type()));
                    if (newField.description() != null) {
                        result.add(
                                SchemaChange.updateColumnComment(
                                        new String[] {newField.name()}, newField.description()));
                    }
                }
            } else {
                result.add(
                        SchemaChange.addColumn(
                                newField.name(), newField.type(), newField.description(), null));
            }
        }

        // option change
        return result;
    }

    @Override
    public void close() throws Exception {
        if (catalog != null) {
            catalog.close();
            catalog = null;
        }
    }
}
