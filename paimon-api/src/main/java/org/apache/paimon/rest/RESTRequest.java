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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;

/** Interface to mark a REST request. */
public interface RESTRequest extends RESTMessage {

    /**
     * Whether sending this request a second time leaves the server where sending it once does.
     *
     * <p>This is how the client treats the request, not something the server is told: it is a
     * getter on a serialized type and must stay off the wire.
     *
     * <p>POST is not idempotent by method, but nearly every request Paimon sends over it is by
     * content — registering a partition, creating a database, committing a snapshot the server
     * already holds — so the client retries them after a 429 or a 503, which is the only defence
     * against a rate limiter or a restarting node. A request that reports an increment is the
     * exception: a proxy answering 503 after the server already applied it turns an automatic retry
     * into a double count that no caller can see. Such a request says so here and is sent exactly
     * once; the failure reaches the caller, which can decide.
     */
    @JsonIgnore
    default boolean isRetrySafe() {
        return true;
    }
}
