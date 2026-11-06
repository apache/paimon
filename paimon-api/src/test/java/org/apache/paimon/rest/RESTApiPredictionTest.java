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

import org.apache.paimon.catalog.Prediction;
import org.apache.paimon.catalog.PredictionSpec;
import org.apache.paimon.catalog.PredictionStatus;
import org.apache.paimon.rest.requests.SubmitPredictionRequest;
import org.apache.paimon.rest.responses.GetPredictionResponse;
import org.apache.paimon.rest.responses.ListPredictionsResponse;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Wire-format tests for asynchronous table prediction metadata. */
class RESTApiPredictionTest {

    @Test
    void testRequestAndResponseWireFormat() throws Exception {
        SubmitPredictionRequest request = new SubmitPredictionRequest(spec());
        Map<String, Object> requestMap = RESTApi.fromJson(RESTApi.toJson(request), Map.class);
        assertThat(requestMap)
                .containsEntry("requestId", "request-1")
                .containsEntry("model", "quality.qwen_vl")
                .doesNotContainKeys("name", "where", "snapshotId", "tagName");
        assertThat(requestMap.get("resultColumns")).isEqualTo(Collections.singletonList("clip_id"));

        GetPredictionResponse response = new GetPredictionResponse(prediction());
        GetPredictionResponse decoded =
                RESTApi.fromJson(RESTApi.toJson(response), GetPredictionResponse.class);
        assertThat(decoded.toPrediction().status()).isEqualTo(PredictionStatus.PENDING);
        assertThat(decoded.getName()).isNull();

        ListPredictionsResponse empty = RESTApi.fromJson("{}", ListPredictionsResponse.class);
        assertThat(empty.getPredictions()).isEmpty();
        assertThat(RESTApi.fromJson(RESTApi.toJson(empty), Map.class))
                .containsOnlyKeys("predictions")
                .containsEntry("predictions", Collections.emptyList());
    }

    @Test
    void testSpecValidation() {
        assertThatThrownBy(
                        () ->
                                new PredictionSpec(
                                        null,
                                        "request-1",
                                        "quality.qwen_vl",
                                        Collections.emptyMap(),
                                        Collections.emptyList(),
                                        "Classify",
                                        Collections.singletonMap("result", "STRING"),
                                        null,
                                        null,
                                        null,
                                        Collections.emptyMap()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inputs must not be empty");

        assertThatThrownBy(
                        () ->
                                new PredictionSpec(
                                        null,
                                        "request-1",
                                        "quality.qwen_vl",
                                        Collections.singletonMap("video", "content"),
                                        Arrays.asList("clip_id", "clip_id"),
                                        "Classify",
                                        Collections.singletonMap("result", "STRING"),
                                        null,
                                        null,
                                        null,
                                        Collections.emptyMap()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("resultColumns must not contain duplicates");
    }

    @Test
    void testTerminalStates() {
        assertThat(PredictionStatus.PENDING.isTerminal()).isFalse();
        assertThat(PredictionStatus.RUNNING.isTerminal()).isFalse();
        assertThat(PredictionStatus.SUCCEEDED.isTerminal()).isTrue();
        assertThat(PredictionStatus.PARTIAL_SUCCESS.isTerminal()).isTrue();
        assertThat(PredictionStatus.FAILED.isTerminal()).isTrue();
        assertThat(PredictionStatus.CANCELLED.isTerminal()).isTrue();
    }

    private static PredictionSpec spec() {
        Map<String, String> inputs = new LinkedHashMap<>();
        inputs.put("video", "content");
        return new PredictionSpec(
                null,
                "request-1",
                "quality.qwen_vl",
                inputs,
                Collections.singletonList("clip_id"),
                "Classify",
                Collections.singletonMap("result", "STRING"),
                null,
                null,
                null,
                Collections.emptyMap());
    }

    private static Prediction prediction() {
        return new Prediction(
                "job-1",
                null,
                PredictionStatus.PENDING,
                7,
                "quality.qwen_vl",
                "1",
                "system.prediction_results",
                10,
                null,
                null,
                null,
                null,
                null,
                null);
    }
}
