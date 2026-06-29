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

package org.apache.paimon.stats;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link StatisticsBlobMetadata}. */
public class StatisticsBlobMetadataTest {

    @Test
    public void testJsonRoundTrip() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("ndv", "1024");
        properties.put("sketch", "theta");

        StatisticsBlobMetadata metadata =
                new StatisticsBlobMetadata(
                        "paimon-ndv-theta-sketch-v1",
                        Arrays.asList(1, 3),
                        10L,
                        20L,
                        properties,
                        "statistics/stats-00001.bin",
                        128L,
                        4096L);

        String json = metadata.toJson();
        assertThat(json).contains("\"type\"", "\"fieldIds\"", "\"fileLocation\"");

        StatisticsBlobMetadata parsed = StatisticsBlobMetadata.fromJson(json);
        assertThat(parsed).isEqualTo(metadata);
        assertThat(parsed.fieldIds()).containsExactly(1, 3);
        assertThat(parsed.snapshotId()).hasValue(10L);
        assertThat(parsed.sequenceNumber()).hasValue(20L);
        assertThat(parsed.properties())
                .containsEntry("ndv", "1024")
                .containsEntry("sketch", "theta");
        assertThat(parsed.offset()).hasValue(128L);
        assertThat(parsed.length()).hasValue(4096L);
    }

    @Test
    public void testOptionalFieldsAndUnknownFields() {
        String json =
                "{\n"
                        + "  \"type\" : \"table-row-count-v1\",\n"
                        + "  \"fileLocation\" : \"statistics/stats-00002.bin\",\n"
                        + "  \"unknown\" : \"ignored\"\n"
                        + "}";

        StatisticsBlobMetadata metadata = StatisticsBlobMetadata.fromJson(json);
        assertThat(metadata.type()).isEqualTo("table-row-count-v1");
        assertThat(metadata.fieldIds()).isEmpty();
        assertThat(metadata.snapshotId().isPresent()).isFalse();
        assertThat(metadata.sequenceNumber().isPresent()).isFalse();
        assertThat(metadata.properties()).isEmpty();
        assertThat(metadata.fileLocation()).isEqualTo("statistics/stats-00002.bin");
        assertThat(metadata.offset().isPresent()).isFalse();
        assertThat(metadata.length().isPresent()).isFalse();

        String roundTripJson = metadata.toJson();
        assertThat(roundTripJson).doesNotContain("\"fieldIds\"", "\"properties\"");
        assertThat(StatisticsBlobMetadata.fromJson(roundTripJson)).isEqualTo(metadata);
    }

    @Test
    public void testMissingRequiredFields() {
        assertThatThrownBy(
                        () ->
                                StatisticsBlobMetadata.fromJson(
                                        "{\n"
                                                + "  \"fileLocation\" : \"statistics/stats-00002.bin\"\n"
                                                + "}"))
                .hasMessageContaining("Missing required field 'type'");

        assertThatThrownBy(
                        () ->
                                StatisticsBlobMetadata.fromJson(
                                        "{\n" + "  \"type\" : \"table-row-count-v1\"\n" + "}"))
                .hasMessageContaining("Missing required field 'fileLocation'");
    }

    @Test
    public void testCollectionDefensiveCopy() {
        List<Integer> fieldIds = new ArrayList<>(Arrays.asList(1, 3));

        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("ndv", "1024");

        StatisticsBlobMetadata metadata =
                new StatisticsBlobMetadata(
                        "paimon-ndv-theta-sketch-v1",
                        fieldIds,
                        10L,
                        20L,
                        properties,
                        "statistics/stats-00001.bin",
                        128L,
                        4096L);

        fieldIds.set(0, 2);
        fieldIds.add(4);
        properties.put("sketch", "theta");

        assertThat(metadata.fieldIds()).containsExactly(1, 3);
        assertThat(metadata.properties()).containsOnlyKeys("ndv");
        assertThatThrownBy(() -> metadata.fieldIds().add(4))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> metadata.properties().put("new-key", "new-value"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
