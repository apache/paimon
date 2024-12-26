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

package org.apache.paimon.factories;

import org.apache.paimon.format.FileFormatFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

/** Base Factory Util. */
public class BaseFactoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(BaseFactoryUtil.class);

    /**
     * Discover factories.
     *
     * @param classLoader the class loader
     * @param klass the klass
     * @param <T> the type of the factory
     * @return the list of factories
     */
    public static <T> List<T> discoverFactories(ClassLoader classLoader, Class<T> klass) {
        final Iterator<T> serviceLoaderIterator = ServiceLoader.load(klass, classLoader).iterator();

        final List<T> loadResults = new ArrayList<>();
        while (true) {
            try {
                // error handling should also be applied to the hasNext() call because service
                // loading might cause problems here as well
                if (!serviceLoaderIterator.hasNext()) {
                    break;
                }

                loadResults.add(serviceLoaderIterator.next());
            } catch (Throwable t) {
                if (t instanceof NoClassDefFoundError) {
                    LOG.debug(
                            "NoClassDefFoundError when loading a {}. This is expected when trying to load factory but no implementation is loaded.",
                            FileFormatFactory.class.getCanonicalName(),
                            t);
                } else {
                    throw new RuntimeException(
                            "Unexpected error when trying to load service provider.", t);
                }
            }
        }

        return loadResults;
    }
}
