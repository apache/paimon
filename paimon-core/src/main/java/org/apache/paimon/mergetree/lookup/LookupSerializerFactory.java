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

package org.apache.paimon.mergetree.lookup;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.base.Supplier;
import org.apache.paimon.shade.guava30.com.google.common.base.Suppliers;

import javax.annotation.Nullable;

import java.util.function.Function;

/** Factory to create serializer for lookup. */
public interface LookupSerializerFactory {

    String version();

    Function<InternalRow, byte[]> createSerializer(RowType currentSchema);

    Function<byte[], InternalRow> createDeserializer(
            String fileSerVersion, RowType currentSchema, @Nullable RowType fileSchema);

    Supplier<LookupSerializerFactory> INSTANCE =
            Suppliers.memoize(
                    () ->
                            FactoryUtil.discoverSingletonFactory(
                                            LookupSerializerFactory.class.getClassLoader(),
                                            LookupSerializerFactory.class)
                                    .orElseGet(DefaultLookupSerializerFactory::new));
}
