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

package org.apache.paimon.rest.requests;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Request for renaming. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RenameTableRequest implements RESTRequest {

    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_DESTINATION = "destination";

    @JsonProperty(FIELD_SOURCE)
    private final Identifier source;

    @JsonProperty(FIELD_DESTINATION)
    private final Identifier destination;

    @JsonCreator
    public RenameTableRequest(
            @JsonProperty(FIELD_SOURCE) Identifier source,
            @JsonProperty(FIELD_DESTINATION) Identifier destination) {
        this.source = source;
        this.destination = destination;
    }

    @JsonGetter(FIELD_DESTINATION)
    public Identifier getDestination() {
        return destination;
    }

    @JsonGetter(FIELD_SOURCE)
    public Identifier getSource() {
        return source;
    }
}
