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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.Experimental;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Provider-neutral specification for an asynchronous prediction over a table snapshot.
 *
 * <p>{@code options} uses Paimon dynamic table option names. Time travel uses {@code
 * scan.snapshot-id} or {@code scan.tag-name}.
 */
@Experimental
public class PredictionSpec {

    private final @Nullable String name;
    private final Identifier model;
    private final Map<String, String> inputs;
    private final List<String> mergeKeyColumns;
    private final String instructions;
    private final @Nullable String where;
    private final Map<String, String> options;
    private final @Nullable String idempotencyKey;

    public PredictionSpec(
            @Nullable String name,
            Identifier model,
            Map<String, String> inputs,
            @Nullable List<String> mergeKeyColumns,
            String instructions,
            @Nullable String where,
            @Nullable Map<String, String> options,
            @Nullable String idempotencyKey) {
        checkOptionalNonBlank(name, "name");
        checkArgument(model != null, "model must not be null");
        checkNonBlank(model.getDatabaseName(), "model database");
        checkNonBlank(model.getObjectName(), "model object");
        checkNonBlank(instructions, "instructions");
        checkOptionalNonBlank(where, "where");
        checkOptionalNonBlank(idempotencyKey, "idempotencyKey");
        this.name = name;
        this.model = model;
        this.inputs = copyStringMap(inputs, "inputs", true);
        this.mergeKeyColumns = copyOptionalStringList(mergeKeyColumns, "mergeKeyColumns");
        this.instructions = instructions;
        this.where = where;
        this.idempotencyKey = idempotencyKey;
        this.options =
                options == null ? Collections.emptyMap() : copyStringMap(options, "options", false);
        CoreOptions coreOptions = new CoreOptions(this.options);
        Long snapshotId = coreOptions.scanSnapshotId();
        String tagName = coreOptions.scanTagName();
        checkArgument(snapshotId == null || snapshotId > 0, "scan.snapshot-id must be positive");
        checkArgument(
                snapshotId == null || tagName == null,
                "scan.snapshot-id and scan.tag-name are mutually exclusive");
    }

    @Nullable
    public String name() {
        return name;
    }

    public Identifier model() {
        return model;
    }

    public Map<String, String> inputs() {
        return inputs;
    }

    public List<String> mergeKeyColumns() {
        return mergeKeyColumns;
    }

    public String instructions() {
        return instructions;
    }

    @Nullable
    public String where() {
        return where;
    }

    public Map<String, String> options() {
        return options;
    }

    @Nullable
    public String idempotencyKey() {
        return idempotencyKey;
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

    private static List<String> copyOptionalStringList(@Nullable List<String> values, String name) {
        return values == null ? Collections.emptyList() : copyStringList(values, name);
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
