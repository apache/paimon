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

package org.apache.paimon.catalog;

import org.apache.paimon.annotation.Experimental;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Named input or output field of a model definition. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class ModelField {

    private final String name;
    private final String dataType;
    private final @Nullable String modality;

    @JsonCreator
    public ModelField(
            @JsonProperty("name") String name,
            @JsonProperty("dataType") String dataType,
            @Nullable @JsonProperty("modality") String modality) {
        checkNonBlank(name, "name");
        checkNonBlank(dataType, "dataType");
        if (modality != null) {
            checkNonBlank(modality, "modality");
        }
        this.name = name;
        this.dataType = dataType;
        this.modality = modality;
    }

    @JsonGetter("name")
    public String name() {
        return name;
    }

    @JsonGetter("dataType")
    public String dataType() {
        return dataType;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter("modality")
    public String modality() {
        return modality;
    }

    static void checkNonBlank(@Nullable String value, String name) {
        checkArgument(value != null && !value.trim().isEmpty(), "%s must not be blank", name);
    }
}
