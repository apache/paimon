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

package org.apache.paimon.spark.catalog;

import org.apache.paimon.spark.FlussLakeStreamReadTable;
import org.apache.paimon.spark.FlussLakeStreamTable;
import org.apache.paimon.spark.SparkTable;
import org.apache.paimon.table.FileStoreTable;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;

import java.util.Optional;

/** Catalog support for Fluss LakeStream tables backed by Paimon. */
public interface SupportFluss {

    String LAKESTREAM_ENABLED = "lakestream.enabled";
    String REAL_TIME_SUFFIX = "$rt";

    /** Loads a Paimon table, routing LakeStream writes and {@code $rt} reads through Fluss. */
    default Table loadTableWithFluss(
            Identifier identifier, TableLoader paimonTableLoader, TableLoader flussTableLoader)
            throws NoSuchTableException {
        Optional<Identifier> realTimeTableBase = flussLakeStreamBaseIdentifier(identifier);
        if (realTimeTableBase.isPresent()) {
            Identifier baseIdentifier = realTimeTableBase.get();
            Table baseTable;
            try {
                baseTable = paimonTableLoader.load(baseIdentifier);
            } catch (NoSuchTableException e) {
                throw new NoSuchTableException(identifier);
            }
            if (!isFlussLakeStreamTable(baseTable)) {
                throw new NoSuchTableException(identifier);
            }
            return new FlussLakeStreamReadTable(flussTableLoader.load(baseIdentifier));
        }

        Table paimonTable = paimonTableLoader.load(identifier);
        return isFlussLakeStreamTable(paimonTable)
                ? new FlussLakeStreamTable(
                        (SparkTable) paimonTable, flussTableLoader.load(identifier))
                : paimonTable;
    }

    default boolean isFlussLakeStreamTable(Table table) {
        if (!(table instanceof SparkTable)) {
            return false;
        }
        org.apache.paimon.table.Table paimonTable = ((SparkTable) table).getTable();
        return paimonTable instanceof FileStoreTable
                && Boolean.parseBoolean(paimonTable.options().get(LAKESTREAM_ENABLED));
    }

    /** Returns the base identifier of a Fluss {@code $rt} table, if present. */
    default Optional<Identifier> flussLakeStreamBaseIdentifier(Identifier identifier) {
        String tableName = identifier.name();
        int suffixStart = tableName.length() - REAL_TIME_SUFFIX.length();
        if (suffixStart <= 0
                || !tableName.endsWith(REAL_TIME_SUFFIX)
                || tableName.indexOf('$') != suffixStart) {
            return Optional.empty();
        }
        return Optional.of(
                Identifier.of(identifier.namespace(), tableName.substring(0, suffixStart)));
    }

    /** Loads a Spark table for the given identifier. */
    @FunctionalInterface
    interface TableLoader {
        Table load(Identifier identifier) throws NoSuchTableException;
    }
}
