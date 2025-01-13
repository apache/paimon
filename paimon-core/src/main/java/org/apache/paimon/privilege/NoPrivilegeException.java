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

package org.apache.paimon.privilege;

import java.util.Arrays;
import java.util.stream.Collectors;

/** Thrown when tries to perform an operation but the current user does not have the privilege. */
public class NoPrivilegeException extends RuntimeException {
    private final String user;
    private final String objectType;
    private final String identifier;

    public NoPrivilegeException(
            String user, String objectType, String identifier, PrivilegeType... privilege) {
        super(
                String.format(
                        "User %s doesn't have privilege %s on %s %s",
                        user,
                        Arrays.stream(privilege)
                                .map(Enum::name)
                                .collect(Collectors.joining(" or ")),
                        objectType,
                        identifier));
        this.user = user;
        this.objectType = objectType;
        this.identifier = identifier;
    }

    String getUser() {
        return user;
    }

    String getObjectType() {
        return objectType;
    }

    String getIdentifier() {
        return identifier;
    }
}
