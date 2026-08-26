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

import org.apache.paimon.PagedList;
import org.apache.paimon.annotation.Experimental;

import javax.annotation.Nullable;

/** Control-plane contract for row-filter and column-masking policies. */
@Experimental
public interface PolicyManagement {

    PagedList<DataPolicy> listPolicies(ListPoliciesRequest request);

    void createPolicy(DataPolicy policy) throws PolicyAlreadyExistException;

    void dropPolicy(
            PermissionResource resource,
            PolicyType type,
            String principal,
            @Nullable String column,
            boolean ignoreIfNotExists);

    /** Exception for trying to create a policy that already exists. */
    class PolicyAlreadyExistException extends Exception {

        private final DataPolicy policy;

        public PolicyAlreadyExistException(DataPolicy policy) {
            this(policy, null);
        }

        public PolicyAlreadyExistException(DataPolicy policy, Throwable cause) {
            super(message(policy), cause);
            this.policy = policy;
        }

        public DataPolicy policy() {
            return policy;
        }

        private static String message(DataPolicy policy) {
            String target = policy.type().name();
            if (policy.getColumnMask() != null) {
                target += "(" + policy.getColumnMask().getOnColumn() + ")";
            }
            PermissionResource resource = policy.getResource();
            return String.format(
                    "%s policy for principal '%s' already exists on table '%s.%s'.",
                    target, policy.getPrincipal(), resource.getDatabase(), resource.getTable());
        }
    }
}
