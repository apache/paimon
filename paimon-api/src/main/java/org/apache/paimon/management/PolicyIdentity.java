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

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Resource-scoped identity of a named data policy. */
@Experimental
public class PolicyIdentity {

    private final PermissionResource resource;
    private final String name;

    public PolicyIdentity(PermissionResource resource, String name) {
        this.resource = checkNotNull(resource, "resource cannot be null");
        resource.validatePolicyAttachment();
        checkArgument(name != null && !name.trim().isEmpty(), "policy name cannot be empty.");
        this.name = name;
    }

    public static PolicyIdentity fromPolicy(DataPolicy policy) {
        return new PolicyIdentity(policy.getResource(), policy.getName());
    }

    public PermissionResource getResource() {
        return resource;
    }

    public String getName() {
        return name;
    }
}
