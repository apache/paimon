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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import javax.annotation.Nullable;

/** Paged {@link CoordinationResponse} with next page token. */
public class PagedCoordinationResponse implements CoordinationResponse {

    private final byte[] content;
    private final @Nullable Integer nextPageToken;

    public PagedCoordinationResponse(byte[] content, @Nullable Integer nextPageToken) {
        this.content = content;
        this.nextPageToken = nextPageToken;
    }

    public byte[] content() {
        return content;
    }

    @Nullable
    public Integer nextPageToken() {
        return nextPageToken;
    }
}
