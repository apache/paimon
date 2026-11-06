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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Provider-neutral specification for an asynchronous prediction over a table snapshot. */
@Experimental
public class PredictionSpec {

    private final @Nullable String name;
    private final @Nullable String requestId;
    private final String model;
    private final Map<String, String> inputs;
    private final List<String> resultColumns;
    private final String prompt;
    private final Map<String, String> outputSchema;
    private final @Nullable String where;
    private final @Nullable Long snapshotId;
    private final @Nullable String tagName;
    private final Map<String, Object> options;

    public PredictionSpec(
            @Nullable String name,
            @Nullable String requestId,
            String model,
            Map<String, String> inputs,
            List<String> resultColumns,
            String prompt,
            Map<String, String> outputSchema,
            @Nullable String where,
            @Nullable Long snapshotId,
            @Nullable String tagName,
            @Nullable Map<String, Object> options) {
        checkOptionalNonBlank(name, "name");
        checkOptionalNonBlank(requestId, "requestId");
        checkNonBlank(model, "model");
        checkNonBlank(prompt, "prompt");
        checkOptionalNonBlank(where, "where");
        checkOptionalNonBlank(tagName, "tagName");
        checkArgument(snapshotId == null || snapshotId > 0, "snapshotId must be positive");
        checkArgument(
                snapshotId == null || tagName == null,
                "snapshotId and tagName are mutually exclusive");
        this.name = name;
        this.requestId = requestId;
        this.model = model;
        this.inputs = copyStringMap(inputs, "inputs", true);
        this.resultColumns = copyStringList(resultColumns, "resultColumns");
        this.prompt = prompt;
        this.outputSchema = copyStringMap(outputSchema, "outputSchema", true);
        this.where = where;
        this.snapshotId = snapshotId;
        this.tagName = tagName;
        this.options =
                options == null
                        ? Collections.emptyMap()
                        : Collections.unmodifiableMap(new LinkedHashMap<>(options));
    }

    @Nullable
    public String name() {
        return name;
    }

    @Nullable
    public String requestId() {
        return requestId;
    }

    public String model() {
        return model;
    }

    public Map<String, String> inputs() {
        return inputs;
    }

    public List<String> resultColumns() {
        return resultColumns;
    }

    public String prompt() {
        return prompt;
    }

    public Map<String, String> outputSchema() {
        return outputSchema;
    }

    @Nullable
    public String where() {
        return where;
    }

    @Nullable
    public Long snapshotId() {
        return snapshotId;
    }

    @Nullable
    public String tagName() {
        return tagName;
    }

    public Map<String, Object> options() {
        return options;
    }

    private static Map<String, String> copyStringMap(
            Map<String, String> values, String name, boolean nonEmpty) {
        checkArgument(values != null, "%s must not be null", name);
        checkArgument(!nonEmpty || !values.isEmpty(), "%s must not be empty", name);
        Map<String, String> copy = new LinkedHashMap<>();
        values.forEach(
                (key, value) -> {
                    checkNonBlank(key, name + " key");
                    checkNonBlank(value, name + " value");
                    copy.put(key, value);
                });
        return Collections.unmodifiableMap(copy);
    }

    private static List<String> copyStringList(List<String> values, String name) {
        checkArgument(values != null, "%s must not be null", name);
        List<String> copy = new ArrayList<>();
        for (String value : values) {
            checkNonBlank(value, name + " value");
            checkArgument(!copy.contains(value), "%s must not contain duplicates", name);
            copy.add(value);
        }
        return Collections.unmodifiableList(copy);
    }

    private static void checkOptionalNonBlank(@Nullable String value, String name) {
        if (value != null) {
            checkNonBlank(value, name);
        }
    }

    private static void checkNonBlank(String value, String name) {
        checkArgument(value != null && !value.trim().isEmpty(), "%s must not be blank", name);
    }
}
