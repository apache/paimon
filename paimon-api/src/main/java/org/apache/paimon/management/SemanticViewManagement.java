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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.view.SemanticView;
import org.apache.paimon.view.SemanticViewDefinition;

import javax.annotation.Nullable;

import java.util.List;

/** Definition management for semantic views. Registration does not imply query engine support. */
@Experimental
public interface SemanticViewManagement {

    /** Creates or atomically replaces the complete definition, preserving identity and grants. */
    default SemanticView upsertSemanticView(
            Identifier identifier, SemanticViewDefinition definition) {
        return upsertSemanticView(identifier, definition, null);
    }

    /**
     * Upserts unconditionally when expectedRevision is null. Otherwise, atomically replaces an
     * existing matching revision; missing objects fail with 404 and stale revisions with 409. A
     * conflict must be reconciled by the caller, never retried as an unconditional write.
     */
    SemanticView upsertSemanticView(
            Identifier identifier,
            SemanticViewDefinition definition,
            @Nullable String expectedRevision);

    SemanticView getSemanticView(Identifier identifier);

    /** Lists names only, following pagination. */
    default List<String> listSemanticViews(String database) {
        return PagedList.listAllFromPagedApi(
                token -> listSemanticViewsPaged(database, null, token));
    }

    /** Lists names with a page size of 1 to 1000, or the server default when null. */
    PagedList<String> listSemanticViewsPaged(
            String database, @Nullable Integer maxResults, @Nullable String pageToken);

    default void deleteSemanticView(Identifier identifier) {
        deleteSemanticView(identifier, null);
    }

    /**
     * Deletes the object and its direct management bindings, never its sources. A non-null
     * expectedRevision requires an atomic revision match. Missing objects fail with 404.
     */
    void deleteSemanticView(Identifier identifier, @Nullable String expectedRevision);
}
