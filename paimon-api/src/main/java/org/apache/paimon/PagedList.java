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

package org.apache.paimon;

import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Paged List which supports request data from page streaming.
 *
 * @since 1.1.0
 */
public class PagedList<T> {

    private final List<T> elements;

    @Nullable private final String nextPageToken;

    public PagedList(List<T> elements, @Nullable String nextPageToken) {
        this.elements = elements;
        this.nextPageToken = nextPageToken;
    }

    /** An array of element objects. */
    public List<T> getElements() {
        return this.elements;
    }

    /** Page token to retrieve the next page of results. Absent if there are no more pages. */
    @Nullable
    public String getNextPageToken() {
        return this.nextPageToken;
    }

    /** Util method to list all from paged api. */
    public static <T> List<T> listAllFromPagedApi(Function<String, PagedList<T>> pagedApi) {
        List<T> results = new ArrayList<>();
        String pageToken = null;
        do {
            PagedList<T> response = pagedApi.apply(pageToken);
            pageToken = response.getNextPageToken();
            List<T> elements = response.getElements();
            if (elements != null) {
                results.addAll(elements);
            }
            if (pageToken == null || elements == null || elements.isEmpty()) {
                break;
            }
        } while (StringUtils.isNotEmpty(pageToken));
        return results;
    }
}
