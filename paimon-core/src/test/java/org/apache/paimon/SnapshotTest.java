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

package org.apache.paimon;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotTest {

    @Test
    public void testJsonIgnoreProperties() {
        Snapshot.fromJson(
                "{\n"
                        + "  \"version\" : 3,\n"
                        + "  \"id\" : 5,\n"
                        + "  \"schemaId\" : 0,\n"
                        + "  \"baseManifestList\" : null,\n"
                        + "  \"deltaManifestList\" : null,\n"
                        + "  \"changelogManifestList\" : null,\n"
                        + "  \"commitUser\" : null,\n"
                        + "  \"commitIdentifier\" : 0,\n"
                        + "  \"commitKind\" : \"APPEND\",\n"
                        + "  \"timeMillis\" : 1234,\n"
                        + "  \"totalRecordCount\" : null,\n"
                        + "  \"deltaRecordCount\" : null,\n"
                        + "  \"unknownKey\" : 22222\n"
                        + "}");
    }

    @Test
    public void testIgnoreLogOffset() {
        Snapshot snapshot =
                Snapshot.fromJson(
                        "{\n"
                                + "  \"version\" : 3,\n"
                                + "  \"id\" : 5,\n"
                                + "  \"schemaId\" : 0,\n"
                                + "  \"commitIdentifier\" : 0,\n"
                                + "  \"commitKind\" : \"APPEND\",\n"
                                + "  \"timeMillis\" : 1234,\n"
                                + "  \"logOffsets\" : { }\n"
                                + "}");
        assertThat(snapshot.toJson()).doesNotContain("logOffsets");

        snapshot =
                Snapshot.fromJson(
                        "{\n"
                                + "  \"version\" : 3,\n"
                                + "  \"id\" : 5,\n"
                                + "  \"schemaId\" : 0,\n"
                                + "  \"commitIdentifier\" : 0,\n"
                                + "  \"commitKind\" : \"APPEND\",\n"
                                + "  \"timeMillis\" : 1234,\n"
                                + "  \"logOffsets\" : {\"1\" : 2}\n"
                                + "}");
        assertThat(snapshot.toJson()).contains("logOffsets");
    }

    @Test
    public void testIgnoreWatermark() {
        Snapshot snapshot =
                Snapshot.fromJson(
                        "{\n"
                                + "  \"version\" : 3,\n"
                                + "  \"id\" : 5,\n"
                                + "  \"schemaId\" : 0,\n"
                                + "  \"commitIdentifier\" : 0,\n"
                                + "  \"commitKind\" : \"APPEND\",\n"
                                + "  \"timeMillis\" : 1234,\n"
                                + "  \"watermark\" : -9223372036854775808\n"
                                + "}");
        assertThat(snapshot.toJson()).doesNotContain("watermark");

        snapshot =
                Snapshot.fromJson(
                        "{\n"
                                + "  \"version\" : 3,\n"
                                + "  \"id\" : 5,\n"
                                + "  \"schemaId\" : 0,\n"
                                + "  \"commitIdentifier\" : 0,\n"
                                + "  \"commitKind\" : \"APPEND\",\n"
                                + "  \"timeMillis\" : 1234,\n"
                                + "  \"watermark\" : 121312312\n"
                                + "}");
        assertThat(snapshot.toJson()).contains("watermark");
    }
}
