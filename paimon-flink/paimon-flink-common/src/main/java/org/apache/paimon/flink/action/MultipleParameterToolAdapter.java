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

package org.apache.paimon.flink.action;

import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.Collection;

/** An adapter of {@link MultipleParameterTool} that can deal old style key. */
public class MultipleParameterToolAdapter {

    private final MultipleParameterTool params;

    public MultipleParameterToolAdapter(MultipleParameterTool params) {
        this.params = params;
    }

    public boolean has(String key) {
        return params.has(key) || params.has(fallback(key));
    }

    public String get(String key) {
        String result = params.get(key);
        return result == null ? params.get(fallback(key)) : result;
    }

    public Collection<String> getMultiParameter(String key) {
        Collection<String> result = params.getMultiParameter(key);
        return result == null ? params.getMultiParameter(fallback(key)) : result;
    }

    public String fallback(String key) {
        return key.replaceAll("_", "-");
    }

    public String getRequired(String key) {
        String value = get(key);
        if (value == null) {
            throw new IllegalArgumentException(
                    "Argument '"
                            + key
                            + "' is required. Run '<action> --help' for more information.");
        }
        return value;
    }
}
