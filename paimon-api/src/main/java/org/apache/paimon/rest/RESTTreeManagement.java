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

package org.apache.paimon.rest;

import org.apache.paimon.PagedList;
import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.management.TreeManagement;

import javax.annotation.Nullable;

/** REST implementation of tree management, bound to the configured REST catalog prefix. */
@Experimental
public class RESTTreeManagement implements TreeManagement {

    private final RESTApi api;

    public RESTTreeManagement(RESTApi api) {
        this.api = api;
    }

    @Override
    public PagedList<DatabaseReference> listReferencesPaged(
            String databaseName,
            @Nullable DatabaseReferenceType type,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        return api.listDatabaseReferencesPaged(databaseName, type, maxResults, pageToken);
    }

    @Override
    public DatabaseReference getReference(String databaseName, String referenceName) {
        return api.getDatabaseReference(databaseName, referenceName);
    }

    @Override
    public DatabaseReference createReference(
            String databaseName,
            String referenceName,
            DatabaseReferenceType type,
            DatabaseReference source) {
        return api.createDatabaseReference(databaseName, referenceName, type, source);
    }

    @Override
    public DatabaseReference fastForwardBranch(
            String databaseName, String targetBranch, DatabaseReference source) {
        return api.fastForwardDatabaseBranch(databaseName, targetBranch, source);
    }

    @Override
    public DatabaseReference mergeBranch(
            String databaseName, String targetBranch, DatabaseReference source) {
        return api.mergeDatabaseBranch(databaseName, targetBranch, source);
    }

    @Override
    public DatabaseReference deleteReference(
            String databaseName,
            String referenceName,
            @Nullable DatabaseReferenceType expectedType) {
        return api.deleteDatabaseReference(databaseName, referenceName, expectedType);
    }
}
