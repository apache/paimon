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

import java.util.List;

/**
 * Control-plane contract for managing labels attached directly to catalog entities.
 *
 * <p>Entity types are extensible, server-defined strings. Entity names are canonical names within
 * the configured catalog; clients pass them unchanged without splitting or resolving them.
 */
@Experimental
public interface LabelManagement {

    /**
     * Atomically creates or replaces one label on an existing entity, leaving other keys unchanged.
     *
     * @param value label value; an empty string is allowed, null is not
     */
    void upsertLabel(String entityType, String entityName, String key, String value);

    /** Gets one direct binding. A missing entity or label is an error. */
    Label getLabel(String entityType, String entityName, String key);

    /** Lists all direct bindings, following catalog pagination. */
    default List<Label> listLabels(String entityType, String entityName) {
        return PagedList.listAllFromPagedApi(
                pageToken -> listLabelsPaged(entityType, entityName, null, pageToken));
    }

    /**
     * Lists one page of direct bindings. A missing entity is an error.
     *
     * @param maxResults maximum page size, from 1 to 1000; null uses the server default
     * @param pageToken opaque continuation token from the preceding response; null for the first
     *     page
     */
    PagedList<Label> listLabelsPaged(
            String entityType,
            String entityName,
            @Nullable Integer maxResults,
            @Nullable String pageToken);

    /** Removes one binding. An absent key succeeds; a missing entity is an error. */
    void deleteLabel(String entityType, String entityName, String key);
}
