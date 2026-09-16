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
import org.apache.paimon.rest.DatabaseReference;
import org.apache.paimon.rest.DatabaseReferenceType;

import javax.annotation.Nullable;

/** Control-plane contract for database-level writable branches and immutable tags. */
@Experimental
public interface TreeManagement {

    /**
     * Lists one page of references.
     *
     * @param type reference type to include; null includes branches and tags
     * @param maxResults maximum page size; null or zero uses the server default
     * @param pageToken opaque continuation token; null for the first page
     */
    PagedList<DatabaseReference> listReferencesPaged(
            String databaseName,
            @Nullable DatabaseReferenceType type,
            @Nullable Integer maxResults,
            @Nullable String pageToken);

    /** Gets a named branch or tag. A missing reference is an error. */
    DatabaseReference getReference(String databaseName, String referenceName);

    /** Creates a branch or immutable tag from an existing reference in the same database. */
    DatabaseReference createReference(
            String databaseName,
            String referenceName,
            DatabaseReferenceType type,
            DatabaseReference source);

    /** Fast-forwards a branch to another branch or immutable tag in the same database. */
    DatabaseReference fastForwardBranch(
            String databaseName, String targetBranch, DatabaseReference source);

    /**
     * Deletes and returns a named reference. A missing reference is an error.
     *
     * @param expectedType required type of the reference to delete; null omits the type check
     */
    DatabaseReference deleteReference(
            String databaseName,
            String referenceName,
            @Nullable DatabaseReferenceType expectedType);
}
