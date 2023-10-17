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

package org.apache.paimon.flink.utils;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/** Utility methods for {@link TableEnvironment} and its subclasses. */
public class TableEnvironmentUtils {

    /**
     * Invoke {@code TableEnvironmentImpl#executeInternal(List<Transformation<?>>, List<String>)}
     * from a {@link StreamTableEnvironment} instance through reflecting.
     */
    public static TableResult executeInternal(
            StreamTableEnvironment tEnv,
            List<Transformation<?>> transformations,
            List<String> sinkIdentifierNames) {
        Class<?> clazz = tEnv.getClass().getSuperclass();
        try {
            Method executeInternal =
                    clazz.getDeclaredMethod("executeInternal", List.class, List.class);
            executeInternal.setAccessible(true);

            return (TableResult) executeInternal.invoke(tEnv, transformations, sinkIdentifierNames);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(
                    "Failed to get 'TableEnvironmentImpl#executeInternal(List, List)' method "
                            + "from given StreamTableEnvironment instance by Java reflection. This is unexpected.",
                    e);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(
                    "Failed to invoke 'TableEnvironmentImpl#executeInternal(List, List)' method "
                            + "from given StreamTableEnvironment instance by Java reflection. This is unexpected.",
                    e);
        }
    }
}
