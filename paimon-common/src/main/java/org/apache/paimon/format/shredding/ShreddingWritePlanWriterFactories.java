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

package org.apache.paimon.format.shredding;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.shredding.MapSharedShreddingContext;
import org.apache.paimon.data.shredding.MapSharedShreddingUtils;
import org.apache.paimon.data.shredding.MapSharedShreddingWritePlanFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.types.RowType;

import java.util.List;

/** Helpers for composing per-file shredding writer factories. */
public class ShreddingWritePlanWriterFactories {

    private ShreddingWritePlanWriterFactories() {}

    public static FormatWriterFactory wrapMapSharedShredding(
            FormatWriterFactory delegate, RowType rowType, CoreOptions options) {
        List<String> shreddingFields =
                MapSharedShreddingUtils.detectShreddingColumns(rowType, options);
        if (shreddingFields.isEmpty()) {
            return delegate;
        }

        MapSharedShreddingContext context =
                new MapSharedShreddingContext(
                        MapSharedShreddingUtils.buildColumnToNumColumns(shreddingFields, options));
        return new ShreddingWritePlanWriterFactory(
                delegate, new MapSharedShreddingWritePlanFactory(rowType, context));
    }
}
