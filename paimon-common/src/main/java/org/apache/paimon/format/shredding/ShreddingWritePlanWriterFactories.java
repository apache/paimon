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

import org.apache.paimon.data.shredding.MapSharedShreddingContext;
import org.apache.paimon.data.shredding.MapSharedShreddingWritePlanFactory;
import org.apache.paimon.format.variant.VariantShreddingWritePlanFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Set;

/** Creates the single active shredding write plan factory for a format writer. */
public class ShreddingWritePlanWriterFactories {

    private ShreddingWritePlanWriterFactories() {}

    @Nullable
    public static ShreddingWritePlanFactory createWritePlanFactory(
            RowType rowType,
            Options options,
            @Nullable MapSharedShreddingContext mapSharedShreddingContext,
            Set<ShreddingWritePlanType> supportedTypes,
            String formatIdentifier) {
        ShreddingWritePlanFactory activeFactory = null;
        ShreddingWritePlanType activeType = null;

        VariantShreddingWritePlanFactory variantFactory =
                new VariantShreddingWritePlanFactory(rowType, options);
        if (variantFactory.shouldCreateWritePlan()) {
            activeFactory = variantFactory;
            activeType = ShreddingWritePlanType.VARIANT;
        }

        if (mapSharedShreddingContext != null && !mapSharedShreddingContext.isEmpty()) {
            if (activeFactory != null) {
                throw new UnsupportedOperationException(
                        "Composing multiple active shredding write plans is not supported.");
            }
            activeFactory =
                    new MapSharedShreddingWritePlanFactory(rowType, mapSharedShreddingContext);
            activeType = ShreddingWritePlanType.MAP_SHARED_SHREDDING;
        }

        if (activeFactory == null) {
            return null;
        }
        if (!supportedTypes.contains(activeType)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "File format '%s' does not support %s write plans.",
                            formatIdentifier, activeType));
        }
        return activeFactory;
    }
}
