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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Shared validation for model metadata and its REST representation. */
public final class ModelValidation {

    private static final String DATA_TYPE = "dataType";
    private static final String MODALITY = "modality";

    private ModelValidation() {}

    @Nullable
    public static Map<String, Map<String, Object>> copyAndValidateFields(
            @Nullable Map<String, Map<String, Object>> fields, String name, boolean required) {
        if (fields == null) {
            checkArgument(!required, "%s must be a non-empty mapping", name);
            return null;
        }
        checkArgument(!required || !fields.isEmpty(), "%s must be a non-empty mapping", name);

        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            checkNonBlank(fieldName, String.format("%s field name", name));
            Map<String, Object> field = entry.getValue();
            checkArgument(field != null, "%s field '%s' must be a mapping", name, fieldName);
            for (String property : field.keySet()) {
                checkNonBlank(property, String.format("%s field property", name));
            }
            checkNonBlank(
                    field.get(DATA_TYPE), String.format("%s.%s.%s", name, fieldName, DATA_TYPE));
            if (field.containsKey(MODALITY)) {
                checkNonBlank(
                        field.get(MODALITY), String.format("%s.%s.%s", name, fieldName, MODALITY));
            }
            result.put(fieldName, Collections.unmodifiableMap(new LinkedHashMap<>(field)));
        }
        return Collections.unmodifiableMap(result);
    }

    public static Map<String, Object> copyAndValidateProperties(
            @Nullable Map<String, Object> properties) {
        if (properties == null) {
            return Collections.emptyMap();
        }
        for (String property : properties.keySet()) {
            checkNonBlank(property, "property name");
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    private static void checkNonBlank(@Nullable Object value, String name) {
        checkArgument(
                value instanceof String && !((String) value).trim().isEmpty(),
                "%s must be a non-empty string",
                name);
    }
}
