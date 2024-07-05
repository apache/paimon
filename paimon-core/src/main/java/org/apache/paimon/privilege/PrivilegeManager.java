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

/**
 * Interface for the privilege system which supports identity-based and role-based access control.
 *
 * <p>When the privilege system is initialized two special users, root and anonymous, will be
 * created by default.
 *
 * <ul>
 *   <li>root is the user with all privileges. The password of root is set when calling {@link
 *       PrivilegeManager#initializePrivilege}.
 *   <li>anonymous is the default username if no user and password are provided when creating the
 *       catalog. The default password of anonymous is <code>anonymous</code>.
 * </ul>
 *
 * <p>The privilege system also follows a hierarchical model. That is, If a user has a privilege on
 * an identifier A, he also has this privilege on identifier B, where A is a prefix of B.
 * Identifiers can be
 *
 * <ul>
 *   <li>the whole catalog (identifier is an empty string)
 *   <li>a database (identifier is &lt;database-name&gt;)
 *   <li>a table (identifier is &lt;database-name&gt;.&lt;table-name&gt;)
 * </ul>
 */
public interface PrivilegeManager {

    String USER_ROOT = "root";
    String USER_ANONYMOUS = "anonymous";
    String PASSWORD_ANONYMOUS = "anonymous";
    String IDENTIFIER_WHOLE_CATALOG = "";

    /** Check if the privilege system is enabled. */
    boolean privilegeEnabled();

    /**
     * Initialize the privilege system if not enabled. Also creates two special users: root and
     * anonymous.
     */
    void initializePrivilege(String rootPassword);

    /** Create {@code user} with {@code password}. */
    void createUser(String user, String password);

    /** Remove {@code user} from the privilege system. */
    void dropUser(String user);

    /** Grant {@code user} with {@code privilege} on {@code identifier}. */
    void grant(String user, String identifier, PrivilegeType privilege);

    /**
     * Revoke {@code privilege} from {@code user} on {@code identifier}. Note that {@code user} will
     * also lose {@code privilege} on all descendants of {@code identifier}.
     */
    int revoke(String user, String identifier, PrivilegeType privilege);

    /**
     * Notify the privilege system that the identifier of an object is changed from {@code oldName}
     * to {@code newName}.
     */
    void objectRenamed(String oldName, String newName);

    /** Notify the privilege system that the object with {@code identifier} is dropped. */
    void objectDropped(String identifier);

    /**
     * Get {@link PrivilegeChecker} of this privilege system to check if a user has specific
     * privileges on an object.
     */
    PrivilegeChecker getPrivilegeChecker();
}
