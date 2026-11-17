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
import org.apache.paimon.catalog.Prediction;
import org.apache.paimon.catalog.PredictionOutput;
import org.apache.paimon.catalog.PredictionSource;
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

/** Wire-format tests for asynchronous prediction metadata. */
class RESTApiPredictionTest {

    @Test
    void testRequestAndResponseWireFormat() throws Exception {
        SubmitPredictionRequest request =
                new SubmitPredictionRequest(Identifier.create("quality", "clips"), spec());
        Map<String, Object> requestMap = RESTApi.fromJson(RESTApi.toJson(request), Map.class);
        assertThat(requestMap.get("idempotencyKey")).isInstanceOf(String.class);
        assertThat((String) requestMap.get("idempotencyKey")).isNotBlank();
        assertThat(RESTApi.fromJson(RESTApi.toJson(request), Map.class).get("idempotencyKey"))
                .isEqualTo(requestMap.get("idempotencyKey"));
        assertThat(requestMap)
                .containsEntry("instructions", "Classify")
                .doesNotContainKeys(
                        "name",
                        "prompt",
                        "outputSchema",
                        "where",
                        "snapshotId",
                        "tagName",
                        "resultColumns");
        assertThat(requestMap.get("mergeKeyColumns"))
                .isEqualTo(Collections.singletonList("clip_id"));
        assertThat((Map<String, Object>) requestMap.get("source"))
                .containsEntry("database", "quality")
                .containsEntry("table", "clips");
        assertThat((Map<String, Object>) requestMap.get("model"))
                .containsEntry("database", "quality")
                .containsEntry("object", "qwen_vl");
        assertThat((Map<String, Object>) requestMap.get("options"))
                .containsEntry("scan.snapshot-id", "7");

        GetPredictionResponse response = new GetPredictionResponse(prediction());
        GetPredictionResponse decoded =
                RESTApi.fromJson(RESTApi.toJson(response), GetPredictionResponse.class);
        assertThat(decoded.toPrediction().status().state())
                .isEqualTo(PredictionStatus.State.PENDING);
        assertThat(decoded.toPrediction().status().terminationDetails()).isNull();
        assertThat(decoded.getName()).isNull();
        assertThat(RESTApi.fromJson(RESTApi.toJson(response), Map.class))
                .doesNotContainKeys(
                        "createdAt",
                        "startedAt",
                        "finishedAt",
                        "modelVersion",
                        "outputTable",
                        "output",
                        "startTime",
                        "endTime");

        GetPredictionResponse completed =
                new GetPredictionResponse(
                        new Prediction(
                                "prediction-2",
                                new PredictionSource("quality", "clips"),
                                null,
                                new PredictionStatus(
                                        PredictionStatus.State.TERMINATED,
                                        new PredictionStatus.TerminationDetails(
                                                PredictionStatus.TerminationCode.SUCCESS, null)),
                                7,
                                Identifier.create("quality", "qwen_vl"),
                                new PredictionOutput(
                                        new PredictionSource("system", "prediction_results")),
                                10L,
                                20L));
        Map<String, Object> completedMap = RESTApi.fromJson(RESTApi.toJson(completed), Map.class);
        assertThat(completedMap)
                .containsEntry("startTime", 10)
                .containsEntry("endTime", 20)
                .doesNotContainKeys(
                        "createdAt", "startedAt", "finishedAt", "modelVersion", "outputTable");
        Map<String, Object> output = (Map<String, Object>) completedMap.get("output");
        assertThat((Map<String, Object>) output.get("table"))
                .containsEntry("database", "system")
                .containsEntry("table", "prediction_results");

        ListPredictionsResponse empty = RESTApi.fromJson("{}", ListPredictionsResponse.class);
        assertThat(empty.getPredictions()).isEmpty();
        assertThat(RESTApi.fromJson(RESTApi.toJson(empty), Map.class))
                .containsOnlyKeys("predictions")
                .containsEntry("predictions", Collections.emptyList());
    }

    @Test
    void testStructuredModelPreservesDots() throws Exception {
        PredictionSpec spec =
                new PredictionSpec(
                        null,
                        Identifier.create("quality.archive", "qwen.vl"),
                        Collections.singletonMap("video", "content"),
                        null,
                        "Classify",
                        null,
                        Collections.emptyMap(),
                        "retry-1");
        SubmitPredictionRequest request =
                new SubmitPredictionRequest(Identifier.create("quality", "clips"), spec);
        SubmitPredictionRequest decoded =
                RESTApi.fromJson(RESTApi.toJson(request), SubmitPredictionRequest.class);

        assertThat(decoded.getModel()).isEqualTo(Identifier.create("quality.archive", "qwen.vl"));
        assertThat(decoded.getIdempotencyKey()).isEqualTo("retry-1");
    }

    @Test
    void testSpecValidation() {
        assertThatThrownBy(
                        () ->
                                new PredictionSpec(
                                        null,
                                        Identifier.create("quality", "qwen_vl"),
                                        Collections.emptyMap(),
                                        null,
                                        "Classify",
                                        null,
                                        Collections.emptyMap(),
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inputs must not be empty");

        assertThatThrownBy(
                        () ->
                                new PredictionSpec(
                                        null,
                                        Identifier.create("quality", "qwen_vl"),
                                        Collections.singletonMap("video", "content"),
                                        Arrays.asList("clip_id", "clip_id"),
                                        "Classify",
                                        null,
                                        Collections.emptyMap(),
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mergeKeyColumns must not contain duplicates");

        assertThatThrownBy(
                        () ->
                                new PredictionSpec(
                                        null,
                                        Identifier.create("quality", "qwen_vl"),
                                        Collections.singletonMap("video", "content"),
                                        null,
                                        "Classify",
                                        null,
                                        Collections.singletonMap("scan.snapshot-id", "0"),
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scan.snapshot-id must be positive");

        Map<String, String> conflictingOptions = new LinkedHashMap<>();
        conflictingOptions.put("scan.snapshot-id", "7");
        conflictingOptions.put("scan.tag-name", "training");
        assertThatThrownBy(
                        () ->
                                new PredictionSpec(
                                        null,
                                        Identifier.create("quality", "qwen_vl"),
                                        Collections.singletonMap("video", "content"),
                                        null,
                                        "Classify",
                                        null,
                                        conflictingOptions,
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mutually exclusive");

        assertThatThrownBy(
                        () ->
                                new PredictionSpec(
                                        null,
                                        Identifier.create("", "qwen_vl"),
                                        Collections.singletonMap("video", "content"),
                                        null,
                                        "Classify",
                                        null,
                                        Collections.emptyMap(),
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("model database");
    }

    @Test
    void testTerminalStates() {
        assertThat(new PredictionStatus(PredictionStatus.State.PENDING, null).isTerminal())
                .isFalse();
        assertThat(new PredictionStatus(PredictionStatus.State.RUNNING, null).isTerminal())
                .isFalse();
        assertThat(
                        new PredictionStatus(
                                        PredictionStatus.State.TERMINATING,
                                        new PredictionStatus.TerminationDetails(
                                                PredictionStatus.TerminationCode.CANCELED, null))
                                .isTerminal())
                .isFalse();
        PredictionStatus terminated =
                new PredictionStatus(
                        PredictionStatus.State.TERMINATED,
                        new PredictionStatus.TerminationDetails(
                                PredictionStatus.TerminationCode.SUCCESS, null));
        assertThat(terminated.isTerminal()).isTrue();
        assertThat(terminated.terminationDetails().code())
                .isEqualTo(PredictionStatus.TerminationCode.SUCCESS);
        assertThatThrownBy(() -> new PredictionStatus(PredictionStatus.State.TERMINATED, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static PredictionSpec spec() {
        Map<String, String> inputs = new LinkedHashMap<>();
        inputs.put("video", "content");
        return new PredictionSpec(
                null,
                Identifier.create("quality", "qwen_vl"),
                inputs,
                Collections.singletonList("clip_id"),
                "Classify",
                null,
                Collections.singletonMap("scan.snapshot-id", "7"),
                null);
    }

    private static Prediction prediction() {
        return new Prediction(
                "prediction-1",
                new PredictionSource("quality", "clips"),
                null,
                new PredictionStatus(PredictionStatus.State.PENDING, null),
                7,
                Identifier.create("quality", "qwen_vl"),
                null,
                null,
                null);
    }
}
