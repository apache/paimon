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

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** the helper class of specifying the dictionary option for parquet/orc format. */
public class DictionaryOptions {
    private final boolean tableDictionaryEnable;
    private final Map<String, Boolean> fieldsDicOption;

    public DictionaryOptions(
            boolean tableDictionaryEnable, @Nonnull Map<String, Boolean> fieldsDicOption) {
        this.tableDictionaryEnable = tableDictionaryEnable;
        this.fieldsDicOption = fieldsDicOption;
    }

    public List<String> getDisableDicFields(List<String> fieldPaths) {
        Map<String, Boolean> fieldAndDictionaryOpt = mergeFieldsDictionaryOpt(fieldPaths);
        List<String> result = new ArrayList<>();
        for (Map.Entry<String, Boolean> entry : fieldAndDictionaryOpt.entrySet()) {
            if (!entry.getValue()) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    public Map<String, Boolean> mergeFieldsDictionaryOpt(List<String> fieldPaths) {
        Map<String, Boolean> result = new HashMap<>();

        // global table dictionary opt
        for (String fieldName : fieldPaths) {
            result.put(fieldName, tableDictionaryEnable);
        }

        // fields dictionary opt
        Map<String, Boolean> fieldsDicOption = getFieldPathDicOptions(fieldPaths);
        result.putAll(fieldsDicOption);

        return result;
    }

    public Map<String, Boolean> getFieldPathDicOptions(List<String> fieldPaths) {
        Map<String, Boolean> result = new HashMap<>();

        Map<String, List<String>> pathsGroupByField =
                fieldPaths.stream()
                        .collect(
                                Collectors.groupingBy(
                                        path ->
                                                path.contains(".")
                                                        ? path.substring(0, path.indexOf("."))
                                                        : path));

        // Specify the dictionary for the child paths of the field which has dictionary config
        for (Map.Entry<String, Boolean> fieldOption : fieldsDicOption.entrySet()) {
            String field = fieldOption.getKey();
            boolean dictionaryConfig = fieldOption.getValue();
            List<String> paths = pathsGroupByField.get(field);
            if (paths != null) {
                for (String path : paths) {
                    result.put(path, dictionaryConfig);
                }
            }
        }
        return result;
    }

    public Map<String, Boolean> getFieldsDictOption() {
        return fieldsDicOption;
    }

    public boolean hasFieldsDicOption() {
        return !fieldsDicOption.isEmpty();
    }

    public boolean isTableDictionaryEnable() {
        return tableDictionaryEnable;
    }
}
