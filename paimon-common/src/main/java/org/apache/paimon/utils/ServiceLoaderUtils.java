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

package org.apache.paimon.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class ServiceLoaderUtils {

    private static final ConcurrentMap<Pair<Class<?>, ClassLoader>, List<?>> SERVICELOADERS =
            new ConcurrentHashMap<>();

    private ServiceLoaderUtils() {}

    @SuppressWarnings("unchecked")
    public static <T> List<T> getImplements(Class<T> clazz, ClassLoader clazzLoader) {
        if (clazz != null && clazzLoader != null) {
            return (List<T>)
                    SERVICELOADERS.computeIfAbsent(
                            Pair.of(clazz, clazzLoader),
                            p -> {
                                List<T> impl = new ArrayList<>();
                                ServiceLoader.load(p.getLeft(), clazzLoader)
                                        .forEach(i -> impl.add((T) i));
                                return Collections.unmodifiableList(impl);
                            });
        } else if (clazz == null) {
            throw new IllegalArgumentException("Service interface can not be null.");
        } else {
            throw new IllegalArgumentException("ClassLoader can not be null.");
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> remove(Class<T> clazz, ClassLoader clazzLoader) {
        if (clazz != null && clazzLoader != null) {
            List<?> removed = SERVICELOADERS.remove(Pair.of(clazz, clazzLoader));
            return removed != null ? (List<T>) removed : null;
        } else if (clazz == null) {
            throw new IllegalArgumentException("Service interface can not be null.");
        } else {
            throw new IllegalArgumentException("ClassLoader can not be null.");
        }
    }

    public static void reload() {
        SERVICELOADERS.clear();
    }
}
