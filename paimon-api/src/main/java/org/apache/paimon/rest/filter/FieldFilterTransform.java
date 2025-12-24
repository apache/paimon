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

package org.apache.paimon.rest.filter;

import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A {@link FilterTransform} which references a table field (index/name/type) as the transform
 * input.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FieldFilterTransform implements FilterTransform {

    private static final String FIELD_INDEX = "index";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_DATA_TYPE = "dataType";

    private final int index;
    private final String name;
    private final DataType dataType;

    @JsonCreator
    public FieldFilterTransform(
            @JsonProperty(FIELD_INDEX) int index,
            @JsonProperty(FIELD_NAME) String name,
            @JsonProperty(FIELD_DATA_TYPE) DataType dataType) {
        this.index = index;
        this.name = name;
        this.dataType = dataType;
    }

    @JsonGetter(FIELD_INDEX)
    public int index() {
        return index;
    }

    @JsonGetter(FIELD_NAME)
    public String name() {
        return name;
    }

    @JsonGetter(FIELD_DATA_TYPE)
    public DataType dataType() {
        return dataType;
    }
}
