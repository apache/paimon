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

package org.apache.paimon.lumina.index;

/**
 * Factory for the legacy Lumina vector index identifier {@code lumina-vector-ann}.
 *
 * <p>Retained so that tables created before the rename to {@code lumina} continue to load. New
 * tables should use {@link LuminaVectorGlobalIndexerFactory} via the {@code lumina} identifier.
 *
 * @deprecated Use {@link LuminaVectorGlobalIndexerFactory} ({@code lumina}) for new tables. This
 *     factory only exists to keep the legacy identifier resolvable through SPI.
 */
@Deprecated
public class LegacyLuminaVectorGlobalIndexerFactory extends LuminaVectorGlobalIndexerFactory {

    public static final String IDENTIFIER = "lumina-vector-ann";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
