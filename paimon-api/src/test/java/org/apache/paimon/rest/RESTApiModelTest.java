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

import org.apache.paimon.catalog.ModelField;
import org.apache.paimon.catalog.ProviderModelDefinition;
import org.apache.paimon.rest.requests.CreateModelRequest;
import org.apache.paimon.rest.responses.GetModelResponse;
import org.apache.paimon.rest.responses.ListModelsResponse;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Wire-format tests for model metadata requests and responses. */
class RESTApiModelTest {

    @Test
    void testProviderDefinitionRoundTrip() throws Exception {
        CreateModelRequest request =
                new CreateModelRequest("qwen_vl", "Video quality model", definition());
        Map<String, Object> requestJson = RESTApi.fromJson(RESTApi.toJson(request), Map.class);
        assertThat(requestJson).containsOnlyKeys("name", "comment", "definition");
        Map<String, Object> definitionJson = (Map<String, Object>) requestJson.get("definition");
        assertThat(definitionJson)
                .containsEntry("type", "provider")
                .containsEntry("provider", "bailian")
                .containsEntry("providerModel", "qwen3.8-flash")
                .doesNotContainKey("outputs");

        GetModelResponse response =
                RESTApi.fromJson(RESTApi.toJson(request), GetModelResponse.class);
        ProviderModelDefinition definition = (ProviderModelDefinition) response.getDefinition();
        assertThat(definition.inputs()).extracting(ModelField::name).containsExactly("video");
        assertThat(definition.options()).containsEntry("enable_thinking", "false");
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
    void testInvalidProviderDefinitionIsRejected() {
        assertThatThrownBy(
                        () ->
                                new ProviderModelDefinition(
                                        "", "model", fields("text", "STRING", null), null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("provider");
        assertThatThrownBy(
                        () ->
                                new ProviderModelDefinition(
                                        "provider",
                                        " ",
                                        fields("text", "STRING", null),
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("providerModel");
        assertThatThrownBy(
                        () ->
                                new ProviderModelDefinition(
                                        "provider", "model", Collections.emptyList(), null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inputs");
        assertThatThrownBy(() -> new ModelField("text", "", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dataType");
        assertThatThrownBy(() -> new ModelField("video", "BLOB", " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("modality");

        List<ModelField> duplicateFields =
                Arrays.asList(
                        new ModelField("text", "STRING", null),
                        new ModelField("text", "STRING", null));
        assertThatThrownBy(
                        () ->
                                new ProviderModelDefinition(
                                        "provider", "model", duplicateFields, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate field");

        Map<String, String> invalidOptions = new HashMap<>();
        invalidOptions.put("", "value");
        assertThatThrownBy(
                        () ->
                                new ProviderModelDefinition(
                                        "provider",
                                        "model",
                                        fields("text", "STRING", null),
                                        null,
                                        invalidOptions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("option name");
    }

    @Test
    void testProviderDefinitionIsDefensivelyCopied() {
        List<ModelField> inputs = new ArrayList<>();
        inputs.add(new ModelField("text", "STRING", null));
        Map<String, String> options = new HashMap<>();
        options.put("key", "value");

        ProviderModelDefinition definition =
                new ProviderModelDefinition("provider", "model", inputs, null, options);
        inputs.clear();
        options.clear();

        assertThat(definition.inputs()).extracting(ModelField::name).containsExactly("text");
        assertThat(definition.options()).containsEntry("key", "value");
        assertThatThrownBy(() -> definition.inputs().add(new ModelField("extra", "STRING", null)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static ProviderModelDefinition definition() {
        return new ProviderModelDefinition(
                "bailian",
                "qwen3.8-flash",
                fields("video", "BLOB", "video"),
                null,
                Collections.singletonMap("enable_thinking", "false"));
    }

    private static List<ModelField> fields(String name, String dataType, String modality) {
        return Collections.singletonList(new ModelField(name, dataType, modality));
    }
}
