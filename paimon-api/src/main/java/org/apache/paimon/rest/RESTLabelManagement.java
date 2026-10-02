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

import org.apache.paimon.PagedList;
import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.management.Label;
import org.apache.paimon.management.LabelManagement;
import org.apache.paimon.rest.responses.GetLabelResponse;

import javax.annotation.Nullable;

import java.util.stream.Collectors;

/** REST implementation of label management, bound to the configured REST catalog prefix. */
@Experimental
public class RESTLabelManagement implements LabelManagement {

    private final RESTApi api;

    public RESTLabelManagement(RESTApi api) {
        this.api = api;
    }

    @Override
    public void upsertLabel(String entityType, String entityName, String key, String value) {
        api.upsertLabel(entityType, entityName, key, value);
    }

    @Override
    public Label getLabel(String entityType, String entityName, String key) {
        return toLabel(api.getLabel(entityType, entityName, key));
    }

    @Override
    public PagedList<Label> listLabelsPaged(
            String entityType,
            String entityName,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        PagedList<GetLabelResponse> response =
                api.listLabelsPaged(entityType, entityName, maxResults, pageToken);
        return new PagedList<>(
                response.getElements().stream()
                        .map(RESTLabelManagement::toLabel)
                        .collect(Collectors.toList()),
                response.getNextPageToken());
    }

    @Override
    public void deleteLabel(String entityType, String entityName, String key) {
        api.deleteLabel(entityType, entityName, key);
    }

    private static Label toLabel(GetLabelResponse response) {
        return new Label(
                response.getEntityType(),
                response.getEntityName(),
                response.getKey(),
                response.getValue());
    }
}
