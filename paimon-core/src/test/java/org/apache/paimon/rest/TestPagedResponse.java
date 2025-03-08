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

import org.apache.paimon.rest.responses.PagedResponse;

import java.util.List;

/** test for page response. */
public class TestPagedResponse implements PagedResponse<Integer> {

    private final String nextPageToken;
    private final List<Integer> data;

    public TestPagedResponse(String nextPageToken, List<Integer> data) {
        this.nextPageToken = nextPageToken;
        this.data = data;
    }

    @Override
    public List<Integer> data() {
        return this.data;
    }

    @Override
    public String getNextPageToken() {
        return this.nextPageToken;
    }
}
