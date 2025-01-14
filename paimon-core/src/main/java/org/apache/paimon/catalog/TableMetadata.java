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

import org.apache.paimon.schema.TableSchema;

import javax.annotation.Nullable;

/** Metadata for table. */
public class TableMetadata {

    private final TableSchema schema;
    @Nullable private final String uuid;

    public TableMetadata(TableSchema schema, @Nullable String uuid) {
        this.schema = schema;
        this.uuid = uuid;
    }

    public TableSchema schema() {
        return schema;
    }

    @Nullable
    public String uuid() {
        return uuid;
    }

    /** Loader to load {@link TableMetadata}. */
    public interface Loader {
        TableMetadata load(Identifier identifier) throws Catalog.TableNotExistException;
    }
}
