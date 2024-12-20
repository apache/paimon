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

package org.apache.paimon.rest.requests;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.RESTRequest;
import org.apache.paimon.schema.Schema;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Request for creating table. */
public class CreateTableRequest implements RESTRequest {

    private static final String FIELD_DATABASE_NAME = "database";
    private static final String FIELD_TABLE_NAME = "table";
    private static final String FIELD_BRANCH_NAME = "branch";
    private static final String FIELD_SCHEMA = "schema";

    @JsonProperty(FIELD_DATABASE_NAME)
    private String databaseName;

    @JsonProperty(FIELD_TABLE_NAME)
    private String tableName;

    @JsonProperty(FIELD_BRANCH_NAME)
    private String branchName;

    @JsonProperty(FIELD_SCHEMA)
    private TableSchema schema;

    @JsonCreator
    public CreateTableRequest(
            @JsonProperty(FIELD_DATABASE_NAME) String databaseName,
            @JsonProperty(FIELD_TABLE_NAME) String tableName,
            @JsonProperty(FIELD_BRANCH_NAME) String branchName,
            @JsonProperty(FIELD_SCHEMA) TableSchema schema) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.branchName = branchName;
        this.schema = schema;
    }

    public CreateTableRequest(Identifier identifier, Schema schema) {
        this(
                identifier.getDatabaseName(),
                identifier.getTableName(),
                identifier.getBranchName(),
                new TableSchema(schema));
    }

    @JsonGetter(FIELD_DATABASE_NAME)
    public String getDatabaseName() {
        return databaseName;
    }

    @JsonGetter(FIELD_TABLE_NAME)
    public String getTableName() {
        return tableName;
    }

    @JsonGetter(FIELD_BRANCH_NAME)
    public String getBranchName() {
        return branchName;
    }

    @JsonGetter(FIELD_SCHEMA)
    public TableSchema getSchema() {
        return schema;
    }
}
