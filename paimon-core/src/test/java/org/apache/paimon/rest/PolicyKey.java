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

import org.apache.paimon.management.DataPolicy;
import org.apache.paimon.management.PolicyIdentity;
import org.apache.paimon.management.PolicyType;

import javax.annotation.Nullable;

import java.util.Objects;

/** Stable identity of a policy stored by the REST catalog test server. */
final class PolicyKey implements Comparable<PolicyKey> {

    final String tableUuid;
    final PolicyType type;
    final String principal;
    @Nullable final String column;

    PolicyKey(String tableUuid, DataPolicy policy) {
        this(
                tableUuid,
                policy.type(),
                policy.getPrincipal(),
                policy.getColumnMask() == null ? null : policy.getColumnMask().getOnColumn());
    }

    PolicyKey(String tableUuid, PolicyIdentity identity) {
        this(tableUuid, identity.getType(), identity.getPrincipal(), identity.getColumn());
    }

    private PolicyKey(
            String tableUuid, PolicyType type, String principal, @Nullable String column) {
        this.tableUuid = tableUuid;
        this.type = type;
        this.principal = principal;
        this.column = column;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PolicyKey)) {
            return false;
        }
        PolicyKey that = (PolicyKey) o;
        return tableUuid.equals(that.tableUuid)
                && type == that.type
                && principal.equals(that.principal)
                && Objects.equals(column, that.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableUuid, type, principal, column);
    }

    @Override
    public int compareTo(PolicyKey that) {
        int result = tableUuid.compareTo(that.tableUuid);
        if (result != 0) {
            return result;
        }
        result = type.compareTo(that.type);
        if (result != 0) {
            return result;
        }
        result = principal.compareTo(that.principal);
        if (result != 0) {
            return result;
        }
        if (column == null) {
            return that.column == null ? 0 : -1;
        }
        return that.column == null ? 1 : column.compareTo(that.column);
    }
}
