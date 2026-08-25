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
import org.apache.paimon.management.ListPermissionsRequest;
import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PermissionIdentity;
import org.apache.paimon.management.PermissionManagement;
import org.apache.paimon.rest.responses.ListPermissionsResponse;

/** REST implementation of permission management, bound to the configured REST catalog prefix. */
@Experimental
public class RESTPermissionManagement implements PermissionManagement {

    private final RESTApi api;

    public RESTPermissionManagement(RESTApi api) {
        this.api = api;
    }

    @Override
    public PagedList<PermissionAssignment> listPermissions(ListPermissionsRequest request) {
        ListPermissionsResponse response = api.listPermissions(request);
        return new PagedList<>(response.getPermissions(), response.getNextPageToken());
    }

    @Override
    public void grantPermission(PermissionAssignment assignment) {
        api.grantPermission(assignment);
    }

    @Override
    public void revokePermission(PermissionIdentity identity) {
        api.revokePermission(identity);
    }
}
