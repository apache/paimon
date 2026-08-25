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

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Resource-scoped identity of one principal's row filter or column mask. */
@Experimental
public class PolicyIdentity {

    private final PermissionResource resource;
    private final PolicyType type;
    private final String principal;
    @Nullable private final String column;

    public PolicyIdentity(
            PermissionResource resource,
            PolicyType type,
            String principal,
            @Nullable String column) {
        this.resource = checkNotNull(resource, "resource cannot be null");
        resource.validatePolicyAttachment();
        this.type = checkNotNull(type, "policy type cannot be null");
        this.principal = PermissionAssignment.validatePrincipal(principal);
        if (type == PolicyType.ROW_FILTER) {
            checkArgument(isBlank(column), "ROW_FILTER identity cannot contain a column.");
            this.column = null;
        } else {
            checkArgument(!isBlank(column), "column is required for COLUMN_MASKING identity.");
            this.column = column;
        }
    }

    public static PolicyIdentity fromPolicy(DataPolicy policy) {
        ColumnMask columnMask = policy.getColumnMask();
        return new PolicyIdentity(
                policy.getResource(),
                policy.type(),
                policy.getPrincipal(),
                columnMask == null ? null : columnMask.getOnColumn());
    }

    public PermissionResource getResource() {
        return resource;
    }

    public PolicyType getType() {
        return type;
    }

    public String getPrincipal() {
        return principal;
    }

    @Nullable
    public String getColumn() {
        return column;
    }

    /** Stable wire identity used in error responses. */
    public String resourceName() {
        return type.name() + ":" + principal + (column == null ? "" : ":" + column);
    }

    private static boolean isBlank(@Nullable String value) {
        return value == null || value.trim().isEmpty();
    }
}
