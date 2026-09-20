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
import org.apache.paimon.rest.responses.GetDatabaseTagResponse;

import javax.annotation.Nullable;

import java.util.List;

/** Database branch and tag management using the REST catalog's configuration. */
@Experimental
public class RESTTreeManagement implements TreeManagement {

    private final RESTApi api;

    public RESTTreeManagement(RESTApi api) {
        this.api = api;
    }

    @Override
    public List<String> listBranches(String databaseName) {
        return api.listDatabaseBranches(databaseName);
    }

    @Override
    public void createBranch(String databaseName, String branch, @Nullable String fromTag) {
        api.createDatabaseBranch(databaseName, branch, fromTag);
    }

    @Override
    public void dropBranch(String databaseName, String branch) {
        api.dropDatabaseBranch(databaseName, branch);
    }

    @Override
    public void fastForward(String databaseName, String branch) {
        api.fastForwardDatabase(databaseName, branch);
    }

    @Override
    public void createTag(
            String databaseName,
            String tagName,
            @Nullable String fromBranch,
            @Nullable String timeRetained) {
        api.createDatabaseTag(databaseName, tagName, fromBranch, timeRetained);
    }

    @Override
    public GetDatabaseTagResponse getTag(String databaseName, String tagName) {
        return api.getDatabaseTag(databaseName, tagName);
    }

    @Override
    public PagedList<String> listTagsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tagNamePrefix) {
        return api.listDatabaseTagsPaged(databaseName, maxResults, pageToken, tagNamePrefix);
    }

    @Override
    public void deleteTag(String databaseName, String tagName) {
        api.deleteDatabaseTag(databaseName, tagName);
    }
}
