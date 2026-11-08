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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.Model;
import org.apache.paimon.rest.requests.CreateModelRequest;
import org.apache.paimon.rest.responses.GetModelResponse;
import org.apache.paimon.rest.responses.ListModelsResponse;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Wire-format tests for model metadata requests and responses. */
class RESTApiModelTest {

    @Test
    void testOptionalOutputsAreOmitted() throws Exception {
        Map<String, Map<String, Object>> inputs = inputs();
        CreateModelRequest request =
                new CreateModelRequest(
                        "qwen_vl",
                        "bailian",
                        "qwen3.8-flash",
                        inputs,
                        null,
                        Collections.emptyMap());
        assertThat(RESTApi.fromJson(RESTApi.toJson(request), Map.class))
                .containsOnlyKeys("name", "provider", "model", "inputs", "properties");

        GetModelResponse response =
                new GetModelResponse(
                        "qwen_vl",
                        "bailian",
                        "qwen3.8-flash",
                        inputs,
                        null,
                        Collections.emptyMap());
        assertThat(RESTApi.fromJson(RESTApi.toJson(response), Map.class))
                .containsOnlyKeys("name", "provider", "model", "inputs", "properties");
    }

    @Test
    void testMissingModelsBecomeAnEmptyArray() throws Exception {
        ListModelsResponse response = RESTApi.fromJson("{}", ListModelsResponse.class);
        assertThat(response.data()).isEmpty();
        assertThat(RESTApi.fromJson(RESTApi.toJson(response), Map.class))
                .containsOnlyKeys("models")
                .containsEntry("models", Collections.emptyList());
    }

    @Test
    void testInvalidModelFieldsAreRejected() {
        assertThatThrownBy(() -> model(fields("text", Collections.emptyMap()), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inputs.text.dataType");
        assertThatThrownBy(() -> model(fields(" ", field("STRING", null)), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inputs field name");
        assertThatThrownBy(() -> model(fields("text", null), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a mapping");
        assertThatThrownBy(() -> model(fields("text", field(" ", null)), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inputs.text.dataType");
        assertThatThrownBy(() -> model(fields("video", field("BLOB", " ")), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inputs.video.modality");

        Map<String, Object> invalidFieldProperty = field("STRING", null);
        invalidFieldProperty.put("", true);
        assertThatThrownBy(() -> model(fields("text", invalidFieldProperty), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inputs field property");

        Map<String, Object> invalidProperties = new HashMap<>();
        invalidProperties.put("", true);
        assertThatThrownBy(() -> model(fields("text", field("STRING", null)), invalidProperties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("property name");

        assertThatThrownBy(
                        () ->
                                new CreateModelRequest(
                                        "qwen_vl",
                                        "bailian",
                                        "qwen3.8-flash",
                                        fields("text", Collections.emptyMap()),
                                        null,
                                        Collections.emptyMap()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inputs.text.dataType");
    }

    @Test
    void testModelFieldsAreDefensivelyCopied() {
        Map<String, Object> field = field("STRING", null);
        Model model = model(fields("text", field), null);

        field.clear();

        assertThat(model.inputs().get("text")).containsEntry("dataType", "STRING");
    }

    private static Map<String, Map<String, Object>> inputs() {
        Map<String, Object> field = new HashMap<>();
        field.put("dataType", "BLOB");
        field.put("modality", "video");
        Map<String, Map<String, Object>> inputs = new HashMap<>();
        inputs.put("video", field);
        return inputs;
    }

    private static Model model(
            Map<String, Map<String, Object>> inputs, Map<String, Object> properties) {
        return new Model(
                Identifier.create("quality", "qwen_vl"),
                "bailian",
                "qwen3.8-flash",
                inputs,
                null,
                properties);
    }

    private static Map<String, Map<String, Object>> fields(String name, Map<String, Object> field) {
        Map<String, Map<String, Object>> fields = new HashMap<>();
        fields.put(name, field);
        return fields;
    }

    private static Map<String, Object> field(String dataType, String modality) {
        Map<String, Object> field = new HashMap<>();
        field.put("dataType", dataType);
        if (modality != null) {
            field.put("modality", modality);
        }
        return field;
    }
}
