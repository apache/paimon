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
import org.apache.paimon.rest.responses.GetDatabaseTagResponse;

import javax.annotation.Nullable;

import java.util.List;

/** Database-level extensions of Paimon's table branch and tag operations. */
@Experimental
public interface TreeManagement {

    /** Lists branch names, using the same response as table branch listing. */
    List<String> listBranches(String databaseName);

    /**
     * Creates a branch. Without fromTag, copies main's table schemas without data. With fromTag,
     * copies the membership and table versions captured by that database tag.
     */
    void createBranch(String databaseName, String branch, @Nullable String fromTag);

    /** Drops a database branch. The default main branch is protected. */
    void dropBranch(String databaseName, String branch);

    /**
     * Forwards main to the named branch, extending the table fast-forward operation to the
     * database's tables. The path names the source branch. This replaces target table state; it
     * does not perform conflict resolution. Pause writers and reload tables after publication.
     */
    void fastForward(String databaseName, String branch);

    /**
     * Captures an immutable database tag from a branch. Null fromBranch selects main. There is no
     * database-wide snapshot ID; each table contributes its own captured version.
     */
    void createTag(
            String databaseName,
            String tagName,
            @Nullable String fromBranch,
            @Nullable String timeRetained);

    /** Gets database tag metadata. Table versions are read through the tag-suffixed database. */
    GetDatabaseTagResponse getTag(String databaseName, String tagName);

    /** Lists tag names with the same pagination and prefix filter as table tag listing. */
    PagedList<String> listTagsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tagNamePrefix);

    /** Deletes a database tag without deleting versions retained by another reference. */
    void deleteTag(String databaseName, String tagName);
}
