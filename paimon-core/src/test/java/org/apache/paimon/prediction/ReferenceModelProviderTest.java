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

package org.apache.paimon.prediction;

import org.apache.paimon.catalog.ModelField;
import org.apache.paimon.catalog.ProviderModelDefinition;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ReferenceModelProvider}. */
class ReferenceModelProviderTest {

    @Test
    void testDiscoverAndPredict() throws Exception {
        ProviderModelDefinition definition = definition(ReferenceModelProvider.SAMPLE_MODEL);
        ModelProvider provider = ModelProvider.discover(definition, getClass().getClassLoader());
        Map<String, Object> inputs = new LinkedHashMap<>();
        inputs.put("text", "hello");

        assertThat(provider).isInstanceOf(ReferenceModelProvider.class);
        assertThat(provider.predict(definition, inputs, "Return the input as JSON"))
                .isEqualTo("{\"text\":\"hello\"}");
    }

    @Test
    void testRejectUnsupportedModel() {
        ModelProvider provider = new ReferenceModelProvider();

        assertThatThrownBy(() -> provider.validate(definition("unknown")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("supports only model 'sample'");
    }

    private static ProviderModelDefinition definition(String providerModel) {
        return new ProviderModelDefinition(
                ReferenceModelProvider.IDENTIFIER,
                providerModel,
                Collections.singletonList(new ModelField("text", "STRING", null)),
                Collections.singletonList(new ModelField("response", "STRING", null)),
                Collections.emptyMap());
    }
}
