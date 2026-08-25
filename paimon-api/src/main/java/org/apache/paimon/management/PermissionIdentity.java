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

package org.apache.paimon.management;

import org.apache.paimon.annotation.Experimental;

import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Fields that uniquely identify a permission assignment for revocation. */
@Experimental
public class PermissionIdentity {

    private final PermissionResource resource;
    private final String access;
    private final String principal;

    public PermissionIdentity(PermissionResource resource, String access, String principal) {
        this.resource = checkNotNull(resource, "resource cannot be null");
        this.access = PermissionAccess.canonicalize(resource, access);
        this.principal = PermissionAssignment.validatePrincipal(principal);
    }

    public static PermissionIdentity fromAssignment(PermissionAssignment assignment) {
        return new PermissionIdentity(
                assignment.getResource(), assignment.getAccess(), assignment.getPrincipal());
    }

    public PermissionResource getResource() {
        return resource;
    }

    public String getAccess() {
        return access;
    }

    public String getPrincipal() {
        return principal;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PermissionIdentity)) {
            return false;
        }
        PermissionIdentity that = (PermissionIdentity) o;
        return resource.equals(that.resource)
                && access.equals(that.access)
                && principal.equals(that.principal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resource, access, principal);
    }
}
