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

package org.apache.spark.sql.connector.catalog;

/** Capabilities that can be provided by a {@link TableCatalog} implementation. */
public enum TableCatalogCapability {

    /**
     * Signals that the TableCatalog supports defining generated columns upon table creation in SQL.
     *
     * <p>Without this capability, any create/replace table statements with a generated column
     * defined in the table schema will throw an exception during analysis.
     *
     * <p>A generated column is defined with syntax: {@code colName colType GENERATED ALWAYS AS
     * (expr)}
     *
     * <p>Generation expression are included in the column definition for APIs like {@link
     * TableCatalog#createTable}.
     */
    SUPPORTS_CREATE_TABLE_WITH_GENERATED_COLUMNS,

    /**
     * Signals that the TableCatalog supports defining column default value as expression in
     * CREATE/REPLACE/ALTER TABLE.
     *
     * <p>Without this capability, any CREATE/REPLACE/ALTER TABLE statement with a column default
     * value defined in the table schema will throw an exception during analysis.
     *
     * <p>A column default value is defined with syntax: {@code colName colType DEFAULT expr}
     *
     * <p>Column default value expression is included in the column definition for APIs like {@link
     * TableCatalog#createTable}.
     */
    SUPPORT_COLUMN_DEFAULT_VALUE
}
