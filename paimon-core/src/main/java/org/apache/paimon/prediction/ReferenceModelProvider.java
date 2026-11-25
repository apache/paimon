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

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.catalog.ProviderModelDefinition;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Deterministic local model provider for executable examples and integration tests. */
@Experimental
public class ReferenceModelProvider implements ModelProvider {

    public static final String IDENTIFIER = "reference";
    public static final String SAMPLE_MODEL = "sample";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public void validate(ProviderModelDefinition model) {
        checkArgument(
                IDENTIFIER.equals(model.provider()),
                "Reference provider requires provider '%s'",
                IDENTIFIER);
        checkArgument(
                SAMPLE_MODEL.equals(model.providerModel()),
                "Reference provider supports only model '%s'",
                SAMPLE_MODEL);
    }

    @Override
    public String predict(
            ProviderModelDefinition model, Map<String, Object> inputs, String instructions) {
        validate(model);
        return JsonSerdeUtil.toFlatJson(inputs);
    }
}
