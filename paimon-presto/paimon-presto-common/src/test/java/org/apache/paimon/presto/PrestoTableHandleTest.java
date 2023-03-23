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

package org.apache.paimon.presto;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PrestoTableHandle}. */
public class PrestoTableHandleTest {

    private final JsonCodec<PrestoTableHandle> codec = JsonCodec.jsonCodec(PrestoTableHandle.class);

    @Test
    public void testPrestoTableHandle() throws Exception {
        byte[] serializedTable = TestPrestoUtils.getSerializedTable();
        PrestoTableHandle expected =
                new PrestoTableHandle(
                        "test", "user", serializedTable, TupleDomain.all(), Optional.empty());
        testRoundTrip(expected);
    }

    private void testRoundTrip(PrestoTableHandle expected) {
        String json = codec.toJson(expected);
        PrestoTableHandle actual = codec.fromJson(json);
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getSchemaName()).isEqualTo(expected.getSchemaName());
        assertThat(actual.getTableName()).isEqualTo(expected.getTableName());
        assertThat(actual.getSerializedTable()).isEqualTo(expected.getSerializedTable());
        assertThat(actual.getFilter()).isEqualTo(expected.getFilter());
        assertThat(actual.getProjectedColumns()).isEqualTo(expected.getProjectedColumns());
    }
}
