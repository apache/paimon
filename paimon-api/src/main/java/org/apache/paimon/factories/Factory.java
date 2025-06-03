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

/**
 * Base interface for all kind of factories that create object instances from a list of key-value
 * pairs in Paimon's catalog.
 *
 * <p>A factory is uniquely identified by {@link Class} and {@link #identifier()}.
 *
 * <p>The list of available factories is discovered using Java's Service Provider Interfaces (SPI).
 * Classes that implement this interface can be added to {@code
 * META_INF/services/org.apache.paimon.factories.Factory} in JAR files.
 */
public interface Factory {
    /**
     * Returns a unique identifier among same factory interfaces.
     *
     * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code
     * kafka}). If multiple factories exist for different versions, a version should be appended
     * using "-" (e.g. {@code elasticsearch-7}).
     */
    String identifier();
}
