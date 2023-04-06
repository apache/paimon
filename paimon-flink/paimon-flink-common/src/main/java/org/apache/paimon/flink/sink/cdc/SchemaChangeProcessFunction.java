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

import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * A {@link ProcessFunction} to handle {@link SchemaChange}.
 *
 * <p>NOTE: To avoid concurrent schema changes, the parallelism of this {@link ProcessFunction} must
 * be 1.
 */
public class SchemaChangeProcessFunction extends ProcessFunction<SchemaChange, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaChangeProcessFunction.class);

    private final SchemaManager schemaManager;

    public SchemaChangeProcessFunction(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    @Override
    public void processElement(
            SchemaChange schemaChange, Context context, Collector<Void> collector)
            throws Exception {
        if (schemaChange instanceof SchemaChange.AddColumn) {
            try {
                schemaManager.commitChanges(schemaChange);
            } catch (IllegalArgumentException e) {
                // This is normal. For example when a table is split into multiple database tables,
                // all these tables will be added the same column. However schemaManager can't
                // handle duplicated column adds, so we just catch the exception and log it.
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Failed to perform SchemaChange.AddColumn {}, "
                                    + "possibly due to duplicated column name",
                            schemaChange,
                            e);
                }
            }
        } else if (schemaChange instanceof SchemaChange.UpdateColumnType) {
            SchemaChange.UpdateColumnType updateColumnType =
                    (SchemaChange.UpdateColumnType) schemaChange;
            TableSchema schema =
                    schemaManager
                            .latest()
                            .orElseThrow(
                                    () ->
                                            new RuntimeException(
                                                    "Table does not exist. This is unexpected."));
            int idx = schema.fieldNames().indexOf(updateColumnType.fieldName());
            Preconditions.checkState(
                    idx >= 0,
                    "Field name "
                            + updateColumnType.fieldName()
                            + " does not exist in table. This is unexpected.");
            DataType oldType = schema.fields().get(idx).type();
            DataType newType = updateColumnType.newDataType();
            if (canConvert(oldType, newType)) {
                schemaManager.commitChanges(schemaChange);
            } else {
                throw new UnsupportedOperationException(
                        String.format(
                                "Cannot convert field %s from type %s to %s",
                                updateColumnType.fieldName(), oldType, newType));
            }
        } else if (schemaChange instanceof SchemaChange.UpdateColumnComment) {
            schemaManager.commitChanges(schemaChange);
        } else if (schemaChange instanceof SchemaChange.UpdateColumnNullability) {
            schemaManager.commitChanges(schemaChange);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported schema change class "
                            + schemaChange.getClass().getName()
                            + ", content "
                            + schemaChange);
        }
    }

    private static final List<DataTypeRoot> STRING_TYPES =
            Arrays.asList(DataTypeRoot.CHAR, DataTypeRoot.VARCHAR);
    private static final List<DataTypeRoot> BINARY_TYPES =
            Arrays.asList(DataTypeRoot.BINARY, DataTypeRoot.VARBINARY);
    private static final List<DataTypeRoot> INTEGER_TYPES =
            Arrays.asList(
                    DataTypeRoot.TINYINT,
                    DataTypeRoot.SMALLINT,
                    DataTypeRoot.INTEGER,
                    DataTypeRoot.BIGINT);
    private static final List<DataTypeRoot> FLOATING_POINT_TYPES =
            Arrays.asList(DataTypeRoot.FLOAT, DataTypeRoot.DOUBLE);

    public static boolean canConvert(DataType oldType, DataType newType) {
        int oldIdx = STRING_TYPES.indexOf(oldType.getTypeRoot());
        int newIdx = STRING_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getLength(oldType) <= DataTypeChecks.getLength(newType);
        }

        oldIdx = BINARY_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = BINARY_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getLength(oldType) <= DataTypeChecks.getLength(newType);
        }

        oldIdx = INTEGER_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = INTEGER_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return oldIdx <= newIdx;
        }

        oldIdx = FLOATING_POINT_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = FLOATING_POINT_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return oldIdx <= newIdx;
        }

        return false;
    }
}
