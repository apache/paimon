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

package org.apache.paimon.catalog;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.table.FormatTable;

import java.util.List;

/**
 * Catalog methods for working with file format tables.
 *
 * @see FormatTable
 * @since 0.9.0
 */
@Public
public interface SupportsFormatTables {

    /** Whether format table is enabled. */
    boolean formatTableEnabled();

    /**
     * Get names of all format tables under this database. An empty list is returned if none exists.
     *
     * @return a list of the names of all format tables in this database
     * @throws Catalog.DatabaseNotExistException if the database does not exist
     */
    List<String> listFormatTables(String databaseName) throws Catalog.DatabaseNotExistException;

    /**
     * Return a {@link FormatTable} identified by the given {@link Identifier}.
     *
     * @param identifier Path of the table
     * @return The requested table
     * @throws Catalog.TableNotExistException if the target does not exist
     */
    FormatTable getFormatTable(Identifier identifier) throws Catalog.TableNotExistException;
}
