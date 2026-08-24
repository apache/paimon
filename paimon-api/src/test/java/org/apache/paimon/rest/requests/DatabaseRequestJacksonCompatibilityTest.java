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

package org.apache.paimon.rest.requests;

import org.apache.paimon.rest.RESTApi;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests database requests with Paimon's shaded and external Jackson mappers. */
public class DatabaseRequestJacksonCompatibilityTest {

    private static final String CREATE_JSON =
            "{\"name\":\"warehouse\",\"options\":{\"owner\":\"alice\"}}";
    private static final String ALTER_JSON =
            "{\"removals\":[\"owner\"],\"updates\":{\"comment\":\"analytics\"}}";

    @Test
    void testExternalJacksonDeserializesCreateDatabaseRequest() throws Exception {
        CreateDatabaseRequest request =
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(CREATE_JSON, CreateDatabaseRequest.class);

        assertThat(request.getName()).isEqualTo("warehouse");
        assertThat(request.getOptions()).containsEntry("owner", "alice");
    }

    @Test
    void testExternalJacksonDeserializesAlterDatabaseRequest() throws Exception {
        AlterDatabaseRequest request =
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(ALTER_JSON, AlterDatabaseRequest.class);

        assertThat(request.getRemovals()).containsExactly("owner");
        assertThat(request.getUpdates()).containsEntry("comment", "analytics");
    }

    @Test
    void testShadedJacksonRoundTripsDatabaseRequests() throws Exception {
        CreateDatabaseRequest create =
                new CreateDatabaseRequest("warehouse", Collections.singletonMap("owner", "alice"));
        AlterDatabaseRequest alter =
                new AlterDatabaseRequest(
                        Collections.singletonList("owner"),
                        Collections.singletonMap("comment", "analytics"));

        assertThat(roundTrip(create, CreateDatabaseRequest.class).getName()).isEqualTo("warehouse");
        assertThat(roundTrip(alter, AlterDatabaseRequest.class).getUpdates())
                .containsEntry("comment", "analytics");
    }

    private static <T> T roundTrip(T value, Class<T> type) throws Exception {
        return RESTApi.OBJECT_MAPPER.readValue(
                RESTApi.OBJECT_MAPPER.writeValueAsString(value), type);
    }
}
