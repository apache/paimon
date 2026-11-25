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
import org.apache.paimon.factories.Factory;
import org.apache.paimon.factories.FactoryUtil;

import java.util.Map;

/** Executes a registered provider model for one input row. */
@Experimental
public interface ModelProvider extends Factory {

    /** Validates that this provider can execute the registered model definition. */
    void validate(ProviderModelDefinition model);

    /**
     * Executes one model invocation.
     *
     * <p>The prediction executor owns job lifecycle, snapshot reads and result publication. A
     * provider only invokes the model and returns its response.
     */
    String predict(ProviderModelDefinition model, Map<String, Object> inputs, String instructions)
            throws Exception;

    /** Discovers the provider declared by the registered model definition. */
    static ModelProvider discover(ProviderModelDefinition model, ClassLoader classLoader) {
        return FactoryUtil.discoverFactory(classLoader, ModelProvider.class, model.provider());
    }
}
