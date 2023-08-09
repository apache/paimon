/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** the helper class of specifying the dictionary option for parquet/orc format. */
public class DictionaryOptions {
    private final boolean tableDictionaryEnable;
    private final Map<String, Boolean> fieldsDicOption;

    public DictionaryOptions(boolean tableDictionaryEnable, Map<String, Boolean> fieldsDicOption) {
        this.tableDictionaryEnable = tableDictionaryEnable;
        this.fieldsDicOption = fieldsDicOption;
    }

    public List<String> getDicDisabledFields(List<String> fieldPaths) {
        Map<String, Boolean> result = new HashMap<>();
        for (String fieldName : fieldPaths) {
            result.put(fieldName, tableDictionaryEnable);
        }

        Map<String, List<String>> pathGroupByField =
                fieldPaths.stream()
                        .collect(
                                Collectors.groupingBy(
                                        path ->
                                                path.contains(".")
                                                        ? path.substring(0, path.indexOf("."))
                                                        : path));

        // Specify the dictionary config for the child paths which field has dictionary config.
        for (Map.Entry<String, Boolean> fieldOption : fieldsDicOption.entrySet()) {
            String field = fieldOption.getKey();
            boolean dictionaryConfig = fieldOption.getValue();
            List<String> paths = pathGroupByField.get(field);
            if (paths != null) {
                for (String path : paths) {
                    result.put(path, dictionaryConfig);
                }
            }
        }

        return result.entrySet().stream()
                .filter(entry -> !entry.getValue())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    public Map<String, Boolean> getfieldsDictionaryOption() {
        return fieldsDicOption;
    }

    public boolean isEnableAllFieldsDic() {
        boolean allEnable = fieldsDicOption.entrySet().stream().allMatch(Map.Entry::getValue);
        return tableDictionaryEnable && (fieldsDicOption.isEmpty() || allEnable);
    }
}
