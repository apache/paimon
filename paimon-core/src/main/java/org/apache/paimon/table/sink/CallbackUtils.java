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

package org.apache.paimon.table.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Utils to load callbacks. */
public class CallbackUtils {

    public static List<TagCallback> loadTagCallbacks(CoreOptions coreOptions) {
        return loadCallbacks(coreOptions.tagCallbacks(), TagCallback.class);
    }

    public static List<CommitCallback> loadCommitCallbacks(CoreOptions coreOptions) {
        return loadCallbacks(coreOptions.commitCallbacks(), CommitCallback.class);
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> loadCallbacks(
            Map<String, String> clazzParamMaps, Class<T> expectClass) {
        List<T> result = new ArrayList<>();

        for (Map.Entry<String, String> classParamEntry : clazzParamMaps.entrySet()) {
            String className = classParamEntry.getKey();
            String param = classParamEntry.getValue();

            Class<?> clazz;
            try {
                clazz = Class.forName(className, true, CallbackUtils.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            Preconditions.checkArgument(
                    expectClass.isAssignableFrom(clazz),
                    "Class " + clazz + " must implement " + expectClass);

            try {
                if (param == null) {
                    result.add((T) clazz.newInstance());
                } else {
                    result.add((T) clazz.getConstructor(String.class).newInstance(param));
                }
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to initialize commit callback "
                                + className
                                + (param == null ? "" : " with param " + param),
                        e);
            }
        }
        return result;
    }
}
