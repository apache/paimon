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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.schema.SchemaChange;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Utility class for generating a list of schema changes based on old and new configurations. */
public class ConfigChangeGenerator {

    // Predefined constant for a null value
    private static final String NULL_VALUE = null;

    /**
     * Generates a list of schema changes based on the old and new configurations.
     *
     * @param oldConfig The old configuration map
     * @param newConfig The new configuration map
     * @return A list of schema changes
     */
    public static List<SchemaChange> generateConfigChanges(
            Map<String, String> oldConfig, Map<String, String> newConfig) {
        // Preallocate the size of the changes list based on the maximum possible size
        List<SchemaChange> changes = new ArrayList<>(Math.max(oldConfig.size(), newConfig.size()));

        // Unified set of all keys from both old and new configurations
        Set<String> allKeys = new HashSet<>(oldConfig.keySet());
        allKeys.addAll(newConfig.keySet());

        // Iterate through all keys to identify changes
        allKeys.forEach(
                key -> {
                    String oldValue = oldConfig.getOrDefault(key, NULL_VALUE);
                    String newValue = newConfig.getOrDefault(key, NULL_VALUE);

                    if (!Objects.equals(oldValue, NULL_VALUE)
                            && Objects.equals(newValue, NULL_VALUE)) {
                        changes.add(SchemaChange.removeOption(key));
                    } else if (Objects.equals(oldValue, NULL_VALUE) || !oldValue.equals(newValue)) {
                        changes.add(SchemaChange.setOption(key, newValue));
                    }
                });
        return changes;
    }
}
