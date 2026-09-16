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
import org.apache.paimon.rest.MergeMode;
import org.apache.paimon.rest.TableMergeMode;

import javax.annotation.Nullable;

import java.util.List;

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

    /**
     * Merges a branch or immutable tag into a target branch in the same database.
     *
     * <p>Table entries are merged relative to a common ancestor. Conflicting changes fail the merge
     * without modifying the target; the source reference is never modified. A merge with no changes
     * succeeds. The server automatically fast-forwards when possible.
     */
    default DatabaseReference mergeBranch(
            String databaseName, String targetBranch, DatabaseReference source) {
        return mergeBranch(databaseName, targetBranch, source, null, null);
    }

    /**
     * Merges a branch or immutable tag using default and per-table merge modes.
     *
     * <p>Modes apply to source-side changes to complete table versions, including creation and
     * deletion; table row data is not merged. Per-table modes override the default. Unresolved
     * conflicts leave the target unchanged, and the source is never modified. A successful merge
     * records the source as merged, including changes skipped by {@link MergeMode#DROP}.
     *
     * @param defaultMergeMode mode for tables without an override; null means {@link
     *     MergeMode#NORMAL}
     * @param tableMergeModes per-table overrides; null or empty uses the default for every table
     */
    DatabaseReference mergeBranch(
            String databaseName,
            String targetBranch,
            DatabaseReference source,
            @Nullable MergeMode defaultMergeMode,
            @Nullable List<TableMergeMode> tableMergeModes);

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
