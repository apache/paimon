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
import org.apache.paimon.management.ListPermissionsRequest;
import org.apache.paimon.management.Permission;
import org.apache.paimon.management.PermissionManagement;
import org.apache.paimon.rest.responses.ListPermissionsResponse;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** REST implementation of permission management, bound to one remote catalog identifier. */
public class RESTPermissionManagement implements PermissionManagement {

    private final RESTApi api;
    private final String catalog;

    public RESTPermissionManagement(RESTApi api, String catalog) {
        checkArgument(
                catalog != null && !catalog.isEmpty(), "Management catalog is not configured");
        this.api = api;
        this.catalog = catalog;
    }

    @Override
    public PagedList<Permission> listPermissions(ListPermissionsRequest request) {
        ListPermissionsResponse response = api.listPermissions(catalog, request);
        return new PagedList<>(response.getPermissions(), response.getNextPageToken());
    }

    @Override
    public void grantPermission(Permission permission) {
        api.grantPermission(catalog, permission);
    }

    @Override
    public void revokePermission(Permission permission) {
        api.revokePermission(catalog, permission);
    }
}
