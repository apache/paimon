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

import org.apache.paimon.rest.RESTRequest;
import org.apache.paimon.view.ViewChange;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Request for altering view. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlterViewRequest implements RESTRequest {

    private static final String FIELD_CHANGE = "change";

    @JsonProperty(FIELD_CHANGE)
    private final ViewChange viewChange;

    @JsonCreator
    public AlterViewRequest(@JsonProperty(FIELD_CHANGE) ViewChange viewChange) {
        this.viewChange = viewChange;
    }

    @JsonGetter(FIELD_CHANGE)
    public ViewChange getDialectChange() {
        return viewChange;
    }
}
