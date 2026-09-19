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

package org.apache.paimon.rest.responses;

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.rest.DatabaseReference;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;
import java.util.List;

/** Paged response for database-level branches and tags. */
@Experimental
public class ListDatabaseReferencesResponse implements PagedResponse<DatabaseReference> {

    private static final String FIELD_REFERENCES = "references";
    private static final String FIELD_NEXT_PAGE_TOKEN = "nextPageToken";

    @JsonProperty(FIELD_REFERENCES)
    private final List<DatabaseReference> references;

    @Nullable
    @JsonProperty(FIELD_NEXT_PAGE_TOKEN)
    private final String nextPageToken;

    @JsonCreator
    @ConstructorProperties({FIELD_REFERENCES, FIELD_NEXT_PAGE_TOKEN})
    public ListDatabaseReferencesResponse(
            @JsonProperty(FIELD_REFERENCES) List<DatabaseReference> references,
            @Nullable @JsonProperty(FIELD_NEXT_PAGE_TOKEN) String nextPageToken) {
        this.references = references;
        this.nextPageToken = nextPageToken;
    }

    @JsonGetter(FIELD_REFERENCES)
    public List<DatabaseReference> getReferences() {
        return references;
    }

    @Nullable
    @JsonGetter(FIELD_NEXT_PAGE_TOKEN)
    @Override
    public String getNextPageToken() {
        return nextPageToken;
    }

    @Override
    public List<DatabaseReference> data() {
        return references;
    }
}
